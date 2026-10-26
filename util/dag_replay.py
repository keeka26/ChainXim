"""安全JSON reader及确定性的本地输入重放。

重验声明hash/固定target/依赖及Fig.3–6，在每个原协议边界重算确认/项状态。
不挖矿、不调用网络、不从global补知识，不将accepted/order声明作为输入。
"""

import argparse
import hashlib
import json
import math
from pathlib import Path

from data import Block, Chain
from data.dag_events import ProtocolDelta
from consensus import PoW
from consensus.conflux_codec import validate_block_header
from consensus.conflux_confirmation import ConfirmationTracker, validate_confirmation
from consensus.conflux_protocol import compute_view
from consensus.dependency import DependencyResolver
from miner.conflux_dataitems import ConfluxDataitemState
from .dag_audit import EVENT_SCHEMA, delta_payload


def _loads(text):
    """拒绝非有限常量和重复键，防止读者与写者解释不同的JSON。"""

    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise ValueError("duplicate JSON key: " + key)
            result[key] = value
        return result

    def bad(value):
        raise ValueError("non-finite JSON constant: " + value)

    # 合法JSON指数也可能溢出；parse_constant并不处理1e400。
    def finite_float(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("non-finite JSON number: " + value)
        return result

    return json.loads(
        text, object_pairs_hook=pairs, parse_constant=bad, parse_float=finite_float
    )


def _json(path):
    """文件与JSONL行使用同一严格解码规则。"""
    return _loads(Path(path).read_text(encoding="utf-8"))


def _same(left, right):
    """JSON类型与值均相等；bool不能冒充整数计数/深度。"""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(
            _same(left[k], right[k]) for k in left
        )
    if isinstance(left, (list, tuple)):
        return len(left) == len(right) and all(_same(a, b) for a, b in zip(left, right))
    return left == right


def _bytes(value, *, invalid=False):
    """hex输出边界还原bytes；不排序引用，不重算/覆盖声明hash。"""
    if invalid and isinstance(value, dict) and set(value) == {"invalid_type"}:
        return None
    if (
        type(value) is not str
        or len(value) % 2
        or any(x not in "0123456789abcdef" for x in value)
    ):
        raise ValueError("expected canonical lowercase hex")
    return bytes.fromhex(value)


def decode_block(row):
    """无邻接的原Block/PoW.BlockHead；直接恢复原声明，随后验证。

    不调用构造器规范化refs或生成新编号，否则篡改头/声明ID可能被悄悄修复。
    类型标记只用于重验拒绝输入，不允许其进入accepted图。
    """
    if type(row) is not dict:
        raise ValueError("block payload must be an object")
    if set(row) == {"unsupported_input_type"}:
        return None
    required = {
        "name",
        "blockhash",
        "header_version",
        "miner",
        "timestamp",
        "content",
        "target",
        "nonce",
        "prehash",
        "refhashes",
        "height",
        "dag_rank",
        "isGenesis",
        "isAdversaryBlock",
        "size_MB",
        "segment_num",
    }
    if set(row) != required:
        raise ValueError("incomplete or unknown block payload fields")

    def scalar(value):
        return (
            None
            if isinstance(value, dict) and set(value) == {"invalid_type"}
            else value
        )

    head = object.__new__(PoW.BlockHead)
    for name in ("header_version", "miner", "timestamp", "nonce"):
        setattr(head, name, scalar(row[name]))
    for name in ("content", "target", "prehash"):
        setattr(head, name, _bytes(row[name], invalid=True))
    refs = row["refhashes"]
    head.refhashes = (
        tuple(_bytes(x, invalid=True) for x in refs) if type(refs) is list else None
    )
    block = object.__new__(Block)
    block._Block__blockhead = head
    block._Message__name = scalar(row["name"])
    block.blockhash = _bytes(row["blockhash"], invalid=True)
    block.height, block.dag_rank = scalar(row["height"]), scalar(row["dag_rank"])
    block.isGenesis, block.isAdversaryBlock = scalar(row["isGenesis"]), scalar(
        row["isAdversaryBlock"]
    )
    block.size, block.segment_num = scalar(row["size_MB"]), scalar(row["segment_num"])
    block.parentblock, block.next = None, []
    return block


def validate_settings(settings):
    """冻结重放的网络ID范围、协议和固定难度；不写global_var。"""
    if type(settings) is not dict:
        raise ValueError("replay settings must be a dictionary")
    n = settings.get("miner_num")
    if type(n) is not int or not 0 < n <= 2**31:
        raise ValueError("invalid replay miner_num")
    target = _bytes(settings.get("target"))
    if len(target) != 32:
        raise ValueError("target must be 32 bytes")
    if (
        settings.get("protocol_version") != "conflux_2018_v4"
        or settings.get("header_version") != 2
    ):
        raise ValueError("unsupported replay protocol/header version")
    validate_confirmation(settings.get("confirmation_policy"), settings.get("N"))
    item = settings.get("dataitem_settings", {})
    if (
        type(item) is not dict
        or type(item.get("active")) is not bool
        or type(item.get("queue_enabled")) is not bool
    ):
        raise ValueError("invalid replay dataitem settings")
    cap = item.get("max_block_capacity")
    if type(cap) is not int or cap < 0 or item["active"] and cap == 0:
        raise ValueError("invalid replay item capacity")
    return target


def settings_from_manifest(manifest):
    """只读取本次结构化manifest有效参数，不从追加txt第一次匹配。"""
    d = manifest["dag_settings"]
    c = manifest["consensus_param"]
    item = manifest["dataitem_param"]
    # 原INI目标允许大写；只在reader配置边界规范成hex，不改manifest原值。
    settings = dict(
        miner_num=manifest["miner_num"],
        target=bytes.fromhex(c["target"]).hex(),
        N=c["N"],
        protocol_version=d["protocol_version"],
        header_version=d["header_version"],
        confirmation_policy=d["confirmation_policy"],
        dataitem_settings=dict(
            active=manifest["dataitem_policy"]["active"],
            queue_enabled=item["dataitem_input_interval"] > 0,
            max_block_capacity=item["max_block_capacity"],
        ),
    )
    validate_settings(settings)
    return settings


def _validator(settings):
    """重放复用同一静态规则；创世固定规范，非创世验证PoW/容量。"""
    target = validate_settings(settings)
    item = settings["dataitem_settings"]

    def valid(block):
        if not isinstance(block, Block) or not validate_block_header(block):
            return False
        if block.isGenesis:
            return True
        h = block.blockhead
        return (
            h.target == target
            and 0 <= h.miner < settings["miner_num"]
            and block.blockhash < target
            and (
                not item["active"]
                or not len(h.content) % 8
                and len(h.content) // 8 <= item["max_block_capacity"]
            )
        )

    return valid


def restore_graph(graph, settings):
    """全父resolver重建一份原Chain，再核对本地深度、tips/pivot/epoch/order。"""
    from .dag_output import graph_snapshot

    valid = _validator(settings)
    if (
        type(graph) is not dict
        or set(graph) != {"blocks", "tips", "pivot", "epochs", "order"}
        or type(graph["blocks"]) is not list
    ):
        raise ValueError("invalid graph snapshot schema")
    chain = Chain()
    resolver = DependencyResolver(chain, valid)
    hashes = set()
    for row in graph["blocks"]:
        block = decode_block(row)
        if not valid(block) or block.blockhash in hashes:
            raise ValueError("invalid/duplicate declared block identity or PoW")
        if (
            type(block.isAdversaryBlock) is not bool
            or type(block.name) is not str
            or type(block.size) not in (int, float)
            or not math.isfinite(block.size)
            or block.size < 0
            or type(block.segment_num) is not int
            or block.segment_num < 1
        ):
            raise ValueError("invalid block observation metadata")
        if not block.isGenesis and block.isAdversaryBlock:
            raise ValueError(
                "honest profile requires the legacy block flag to be False"
            )
        hashes.add(block.blockhash)
        resolver.submit(block)
    if not chain.head or resolver.pending or set(chain.block_set) != hashes:
        raise ValueError("missing dependencies in accepted graph snapshot")
    chain.set_protocol_view(compute_view(chain))
    if not _same(graph_snapshot(chain), graph):
        raise ValueError(
            "declared graph depths/tips/pivot/epoch/order disagree with recomputation"
        )
    return chain


def _item_update(state, tracker, point, first, events):
    """按数据项身份计算撤销、首次及再次确认，观察phase保持原Miner标记。"""
    old = state.confirmed or ()
    state.set_confirmed_order(
        tracker.current_order if tracker.policy != "none" else None
    )
    new = state.confirmed or ()
    gained = tuple(x for x in new if x not in set(old))
    lost = tuple(x for x in old if x not in set(new))
    again = [x.hex() for x in gained if x in first]
    for x in gained:
        first.setdefault(x, dict(point))
    if gained or lost:
        event = dict(
            point,
            revoked=[x.hex() for x in lost],
            newly_confirmed=[x.hex() for x in gained],
            reconfirmed=again,
        )
        events.append(event)
        return event
    return None


def replay_node_events(events, settings):
    """原计划接口，返回(Chain,ConfluxView)；附只读审计状态供核对。

    只从完整块输入重建，逐边界核对所有声明差量；合法缺父在有限结束时保留pending。
    一个node_start/连续sequence/node_end缺一不可，不执行随机挖矿或网络重跑。
    """
    from .dag_output import graph_snapshot

    events = list(events)
    if (
        len(events) < 3
        or events[0].get("kind") != "node_start"
        or events[-1].get("kind") != "node_end"
    ):
        raise ValueError("missing start/end terminal event; truncated input log")
    valid = _validator(settings)
    subject = events[0].get("miner_id")
    if type(subject) is not int or not 0 <= subject < settings["miner_num"]:
        raise ValueError("invalid event miner_id")
    if any(
        e.get("schema_version") != EVENT_SCHEMA or e.get("miner_id") != subject
        for e in events
    ):
        raise ValueError("mixed event schema or subjects")
    g = decode_block(events[0]["genesis"])
    if not valid(g) or not g.isGenesis:
        raise ValueError("invalid fixed genesis")
    chain = Chain(subject)
    chain.add_dag_block(g)
    resolver = DependencyResolver(chain, valid)
    tracker = ConfirmationTracker(settings["confirmation_policy"], settings["N"])
    item = settings["dataitem_settings"]
    state = ConfluxDataitemState(item["queue_enabled"]) if item["active"] else None
    known, pending, pivot = set(), set(), ()
    accepted_rounds = {}
    first, item_events = {}, []
    protocol_events = []
    invalid = 0
    last_round = 0
    boundaries = events[1:-1]
    for number, e in enumerate(boundaries, 1):
        if (
            e.get("kind") != "protocol_boundary"
            or type(e.get("sequence")) is not int
            or e["sequence"] != number
        ):
            raise ValueError("non-contiguous or duplicate protocol sequence")
        round = e.get("round")
        phase = e.get("phase")
        if type(round) is not int or round < last_round or type(phase) is not str:
            raise ValueError("invalid event clock/phase")
        if number == 1 and (round != 0 or phase != "initial" or e["blocks"]):
            raise ValueError("first boundary must initialize genesis only")
        last_round = round
        previous_pivot = pivot
        # 自产/接收的来源和协议phase须一致，不能伪装他人块为本节点挖矿。
        if phase not in ("initial", "receive_batch", "self_generated"):
            raise ValueError("unsupported protocol boundary phase")
        generated = e["origin"] == "generated"
        if (
            generated != (phase == "self_generated")
            or generated
            and len(e["blocks"]) != 1
        ):
            raise ValueError("input origin differs from protocol phase")
        for row in e["rejections"]:
            if valid(decode_block(row)):
                raise ValueError("declared rejection actually validates")
            invalid += 1
        for content in e["dataitem_inputs"]:
            if state is None:
                raise ValueError("item input in disabled profile")
            state.ingest(_bytes(content))
        if e["origin"] not in ("received", "generated"):
            raise ValueError("invalid complete input origin")
        for row in e["blocks"]:
            block = decode_block(row)
            if generated and (
                not valid(block)
                or block.blockhead.miner != subject
                or block.blockhead.timestamp != round
            ):
                raise ValueError(
                    "self-generated input has wrong identity or mining round"
                )
            if not valid(block):
                if e["origin"] == "generated":
                    raise ValueError("invalid self-generated block")
                invalid += 1
            resolver.submit(block)
        delta = chain.set_protocol_view(compute_view(chain))
        tracker.update(chain.protocol_view, round, phase)
        now = set(chain.block_set)
        wait = set(resolver.pending)
        expected = dict(
            accepted=sorted(h.hex() for h in now - known),
            pending_added=sorted(h.hex() for h in wait - pending),
            pending_removed=sorted(h.hex() for h in pending - wait),
            protocol_delta=delta_payload(delta),
            pivot_delta=delta_payload(
                ProtocolDelta.between(pivot, chain.protocol_view.pivot)
            ),
            confirmation_delta=delta_payload(tracker.last_delta),
            invalid_inputs=invalid,
        )
        for key, value in expected.items():
            if not _same(e.get(key), value):
                raise ValueError(f"input replay differs at sequence {number}: {key}")
        for h in now - known:
            accepted_rounds.setdefault(h, round)
        known, pending, pivot = now, wait, chain.protocol_view.pivot
        if state is not None:
            state.synchronize(chain)
            point = dict(
                round=round, phase=e.get("observation_phase", phase), sequence=number
            )
            item_event = _item_update(state, tracker, point, first, item_events)
            if not _same(e.get("dataitem_state"), state.summary()) or not _same(
                e.get("dataitem_confirmation_delta"), item_event
            ):
                raise ValueError(
                    "replayed dataitem state differs from declared observation"
                )
        if number > 1 and (delta.removed or delta.added):
            observed = dict(
                round=round,
                phase=e.get("observation_phase", phase),
                sequence=number,
                **delta_payload(delta),
                pivot_changed=previous_pivot != pivot,
            )
            if state is not None:
                observed["dataitems"] = state.summary()
            protocol_events.append(observed)
    end = events[-1]
    for row in end["unprocessed_rejections"]:
        if valid(decode_block(row)):
            raise ValueError("invalid terminal rejection declaration")
        invalid += 1
    for row in end["unprocessed_blocks"]:
        if not valid(decode_block(row)):
            raise ValueError("invalid terminal queued input")
    for content in end["unapplied_dataitem_inputs"]:
        if state is None:
            raise ValueError("terminal item input in disabled profile")
        state.ingest(_bytes(content))
    expected = dict(
        boundary_count=len(boundaries),
        graph=graph_snapshot(chain),
        pending=sorted(h.hex() for h in pending),
        confirmation=tracker.snapshot(),
        invalid_inputs=invalid,
        dataitems=state.snapshot() if state is not None else None,
        dataitem_first_confirmed={h.hex(): dict(v) for h, v in first.items()},
        dataitem_confirmation_events=item_events,
    )
    if any(not _same(end.get(k), v) for k, v in expected.items()):
        raise ValueError("terminal state differs from complete input replay")
    chain.replay_accepted_rounds = accepted_rounds
    chain.replay_protocol_events = protocol_events
    chain.replay_confirmation = tracker
    chain.replay_pending = dict(resolver.pending)
    chain.replay_invalid_inputs = invalid
    chain.replay_dataitems = state
    chain.replay_item_first_confirmed = first
    chain.replay_item_confirmation_events = item_events
    return chain, chain.protocol_view


def write_accepted_dump(path, chains, settings):
    """DAG专用JSON信封保留chain_dump.bin路径；不写可执行pickle。"""
    from .dag_output import graph_snapshot

    validate_settings(settings)
    payload = dict(
        schema_version="chainxim_conflux_accepted_dump_r11_v1",
        scope="accepted-only graphs; not simulation checkpoint",
        settings=settings,
        graphs=[graph_snapshot(c) for c in chains],
    )
    Path(path).write_text(
        json.dumps(payload, sort_keys=True, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def load_accepted_dump(path):
    """只读本版本安全格式；重建/验证所有边，不读取旧pickle或网络状态。"""
    payload = _json(path)
    if payload.get("schema_version") != "chainxim_conflux_accepted_dump_r11_v1":
        raise ValueError("unsupported DAG dump schema")
    return [restore_graph(g, payload["settings"]) for g in payload["graphs"]]


def _validate_common_order_metrics(metrics, manifest):
    """新增：核对可选LD摘要；旧结果无此组字段时继续兼容读取。"""
    keys = (
        "common_order",
        "common_order_at_mining_end",
        "block_throughput_common_order",
        "block_throughput_common_order_after_drain",
    )
    if not any(key in metrics for key in keys):
        return False
    if any(key not in metrics for key in keys):
        raise ValueError("incomplete common-order metric summary")
    observed, mining = (metrics[key] for key in keys[:2])
    if any(type(value) is not int or value < 0 for value in (observed, mining)):
        raise ValueError("common-order counts must be nonnegative integers")
    nodes = metrics["nodes"]
    if len(nodes) != manifest["miner_num"] or any(
        type(row.get("miner_id")) is not int or row["miner_id"] != i
        for i, row in enumerate(nodes)
    ):
        raise ValueError("common-order metric node roster is incomplete")
    if (
        observed > min(row["ordered"] for row in nodes)
        or mining > min(row["accepted"] for row in nodes)
        or mining > metrics["ordered_at_mining_end"]
    ):
        raise ValueError("common-order count exceeds node counts")
    if not manifest["dag_settings"]["drain_rounds"] and observed != mining:
        raise ValueError("common-order clocks differ without drain rounds")
    rounds = metrics["total_round"]
    for key, count in zip(keys[2:], (mining, observed)):
        actual = metrics[key]
        if rounds == 0:
            if actual is not None:
                raise ValueError("zero-round common-order throughput must be null")
        elif (
            type(actual) not in (int, float)
            or not math.isfinite(actual)
            or not math.isclose(actual, count / rounds, rel_tol=1e-12, abs_tol=1e-12)
        ):
            raise ValueError("common-order throughput differs from its count")
    return True


def read_results(path, *, replay=False):
    """本次JSON/schema/SHA/全边/精确计数一致；重放进一步核对历史状态。

    文件摘要是本次run关联和误改检测，并非签名或对恶意重写整包的认证。
    """
    from .dag_output import graph_snapshot, _edges
    from consensus.conflux_confirmation import compute_confirmed_order

    path = Path(path)
    m = _json(path / "run_manifest.json")
    if m.get("schema_version") != "chainxim_conflux_run_r11_v1":
        raise ValueError("unsupported run manifest schema; node input audit required")
    settings = settings_from_manifest(m)
    full = not m["output_settings"]["compact_outputfile"]
    names = {"dag_metrics.json"}
    if full:
        names.update(
            {
                "Chain Data/dag_snapshot.json",
                "Chain Data/chain_dump.bin",
                "Chain Data/dag_events.jsonl",
                "Chain Data/dag_inputs.jsonl",
                "Chain Data/dag_edges.json",
            }
        )
    bindings = m.get("artifact_sha256")
    if type(bindings) is not dict or set(bindings) != names:
        raise ValueError("missing/unknown required artifact digest bindings")
    for name, digest in bindings.items():
        candidate = Path(name)
        if (
            candidate.is_absolute()
            or ".." in candidate.parts
            or not (path / candidate).resolve().is_relative_to(path.resolve())
        ):
            raise ValueError("artifact path must stay inside result directory")
        if (
            type(digest) is not str
            or len(digest) != 64
            or hashlib.sha256((path / candidate).read_bytes()).hexdigest() != digest
        ):
            raise ValueError("artifact digest mismatch: " + name)
    metrics = _json(path / "dag_metrics.json")
    if metrics.get("schema_version") != "chainxim_conflux_metrics_r11_v1":
        raise ValueError("unsupported metrics schema")
    has_common_order = _validate_common_order_metrics(metrics, m)
    result = dict(
        manifest=m, metrics=metrics, snapshot=None, chains=[], replay_verified=False
    )
    if not full:
        if replay:
            raise ValueError("compact output has no complete node inputs")
        return result
    snapshot = _json(path / "Chain Data/dag_snapshot.json")
    if snapshot.get("schema_version") != "chainxim_conflux_snapshot_r11_v1":
        raise ValueError("unsupported snapshot schema")
    nodes = snapshot["nodes"]
    if len(nodes) != settings["miner_num"] or any(
        type(n.get("miner_id")) is not int or n["miner_id"] != i
        for i, n in enumerate(nodes)
    ):
        raise ValueError(
            "snapshot node IDs must be the complete ordered network roster"
        )
    generated = restore_graph(snapshot["generated_graph"], settings)
    chains = [restore_graph(n["accepted_graph"], settings) for n in nodes]
    result["chains"], result["snapshot"] = chains, snapshot
    dump_path = path / "Chain Data/chain_dump.bin"
    dump_settings = _json(dump_path)["settings"]
    # 旧诚实dump含空的成员元数据；读取兼容不恢复其运行策略。
    if dump_settings.pop("adversary_ids", []):
        raise ValueError("honest profile only supports all-honest result dumps")
    if not _same(dump_settings, settings):
        raise ValueError("dump and manifest have different protocol settings")
    if not _same(
        [graph_snapshot(c) for c in load_accepted_dump(dump_path)],
        [graph_snapshot(c) for c in chains],
    ):
        raise ValueError("dump and snapshot are from different states")
    edges = _json(path / "Chain Data/dag_edges.json")
    expected_edges = dict(
        schema_version="chainxim_conflux_edges_r11_v1",
        direction="child -> dependency",
        generated_graph=_edges(generated),
        nodes=[dict(miner_id=i, edges=_edges(c)) for i, c in enumerate(chains)],
    )
    if not _same(edges, expected_edges):
        raise ValueError("edge list disagrees with actual main/reference parents")
    # 当前计数/确认从图重新计算，不能只信已更新的SHA。
    reference = 0
    if (
        m["reference_miner_id"] != reference
        or metrics["reference_miner_id"] != reference
    ):
        raise ValueError("reference must be the smallest honest node ID")
    if metrics["generated"] != len(generated.block_set) - 1:
        raise ValueError("generated metric differs from observer graph")
    if len(metrics["nodes"]) != len(nodes):
        raise ValueError("metric node roster is incomplete")
    for i, (c, n, row) in enumerate(zip(chains, nodes, metrics["nodes"])):
        confirm = compute_confirmed_order(
            c.protocol_view, settings["confirmation_policy"], settings["N"]
        )
        current = n["confirmation"]
        if (
            current["policy"] != settings["confirmation_policy"]
            or not _same(current["N"], settings["N"])
            or not _same(current["current_order"], [h.hex() for h in confirm])
        ):
            raise ValueError(
                "current confirmation declaration differs from installed view"
            )
        expected = dict(
            miner_id=i,
            accepted=len(c.block_set) - 1,
            ordered=len(c.protocol_view.order) - 1,
            pending=len(n["pending_hashes"]),
            confirmed=(
                max(0, len(confirm) - 1)
                if settings["confirmation_policy"] != "none"
                else None
            ),
        )
        if any(not _same(row.get(k), v) for k, v in expected.items()):
            raise ValueError("node metric disagrees with rebuilt graph")
        if i == reference and any(
            not _same(metrics.get(k), v) for k, v in expected.items() if k != "miner_id"
        ):
            raise ValueError("reference metric disagrees with rebuilt graph")
    # 新增：完整结果以重建的节点全序交叉验证LD；compact仅有摘要和文件校验。
    if has_common_order:
        from .dag_analysis import common_order_length

        expected_common = common_order_length(c.protocol_view.order for c in chains)
        if metrics["common_order"] != expected_common:
            raise ValueError("common-order metric disagrees with rebuilt node orders")
    if replay:
        events = [
            _loads(line)
            for line in (path / "Chain Data/dag_inputs.jsonl").read_text().splitlines()
        ]
        if set(e["miner_id"] for e in events) != set(n["miner_id"] for n in nodes):
            raise ValueError("event subjects differ from snapshot nodes")
        old_observations = []
        replayed = []
        for i, n in enumerate(nodes):
            chain, _ = replay_node_events(
                [e for e in events if e["miner_id"] == i], settings
            )
            if (
                not _same(graph_snapshot(chain), n["accepted_graph"])
                or not _same(chain.replay_confirmation.snapshot(), n["confirmation"])
                or not _same(
                    (
                        chain.replay_dataitems.snapshot()
                        if chain.replay_dataitems is not None
                        else None
                    ),
                    n["dataitems"],
                )
                or sorted(h.hex() for h in chain.replay_pending) != n["pending_hashes"]
                or not _same(
                    metrics["nodes"][i]["invalid"], chain.replay_invalid_inputs
                )
                or not _same(
                    n["accepted_rounds"],
                    {h.hex(): r for h, r in chain.replay_accepted_rounds.items()},
                )
                or not _same(
                    n["dataitem_first_confirmed"],
                    {h.hex(): v for h, v in chain.replay_item_first_confirmed.items()},
                )
                or not _same(
                    n["dataitem_confirmation_events"],
                    chain.replay_item_confirmation_events,
                )
                or i == reference
                and not _same(metrics["invalid"], chain.replay_invalid_inputs)
            ):
                raise ValueError("input replay and snapshot/metrics differ")
            observations = [
                ("protocol_delta_observation", e) for e in chain.replay_protocol_events
            ]
            observations += [
                ("confirmation_delta_observation", e)
                for e in chain.replay_confirmation.events
            ]
            observations += [
                ("dataitem_confirmation_observation", e)
                for e in chain.replay_item_confirmation_events
            ]
            for kind, e in sorted(observations, key=lambda x: (x[1]["sequence"], x[0])):
                old_observations.append(
                    dict(
                        schema_version="chainxim_conflux_state_event_r9_v1",
                        kind=kind,
                        miner_id=i,
                        **e,
                    )
                )
            replayed.append(chain)
        declared = [
            _loads(line)
            for line in (path / "Chain Data/dag_events.jsonl").read_text().splitlines()
        ]
        if not _same(declared, old_observations):
            raise ValueError(
                "original state observations disagree with recomputed inputs"
            )
        result["chains"] = replayed
        result["replay_verified"] = True
    return result


def main():
    """分析工具入口；原main.py仿真命令不改变，没有第二个仿真runner。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result_path", type=Path, required=True)
    parser.add_argument("--replay", action="store_true")
    args = parser.parse_args()
    try:
        result = read_results(args.result_path, replay=args.replay)
    except (ValueError, KeyError, OSError, TypeError) as error:
        parser.exit(2, f"DAG replay failed: {error}\n")
    print(
        json.dumps(
            dict(
                schema_version="chainxim_conflux_reader_report_r11_v1",
                result_path=str(args.result_path),
                replay_verified=result["replay_verified"],
                nodes=len(result["chains"]),
                generated=result["metrics"]["generated"],
                ordered=result["metrics"]["ordered"],
                confirmed=result["metrics"]["confirmed"],
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
