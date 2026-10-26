"""原 Results 目录中的基础 Conflux 输出。

只导出观察结果，绝不向节点注入全局图。完整输出支持安全JSON读取和节点输入重放。
"""

import hashlib
import json
import time  # 输出工程成本，不参与语义指标。
import platform
import subprocess
from pathlib import Path

import global_var


def graph_snapshot(chain):
    """原生唯一块和所有边转 JSON；bytes 仅在此输出边界转 hex。"""
    view = chain.protocol_view
    blocks = []
    for block in sorted(
        chain.block_set.values(), key=lambda b: (b.dag_rank, b.blockhash)
    ):
        head = block.blockhead
        blocks.append(
            {
                "name": block.name,
                "blockhash": block.blockhash.hex(),
                "header_version": head.header_version,
                "miner": head.miner,
                "timestamp": head.timestamp,
                "content": head.content.hex(),
                "target": head.target.hex(),
                "nonce": head.nonce,
                "prehash": head.prehash.hex(),
                "refhashes": [h.hex() for h in head.refhashes],
                "height": block.height,
                "dag_rank": block.dag_rank,
                "isGenesis": block.isGenesis,
                "isAdversaryBlock": block.isAdversaryBlock,
                # 保存传播大小元数据，reader不从全局猜测。
                "size_MB": block.size,
                "segment_num": block.segment_num,
            }
        )
    return {
        "blocks": blocks,
        "tips": [b.blockhash.hex() for b in chain.get_tips()],
        "pivot": [h.hex() for h in view.pivot] if view else [],
        "epochs": [[h.hex() for h in epoch] for epoch in view.epochs] if view else [],
        "order": [h.hex() for h in view.order] if view else [],
    }


def _git_identity():
    """本地实验来源，dirty 状态和源文件摘要避免把未提交运行误标为 HEAD。"""
    root = Path(__file__).resolve().parents[1]

    def git(*args):
        return subprocess.check_output(["git", *args], cwd=root, text=True).strip()

    try:
        identity = {
            "commit": git("rev-parse", "HEAD"),
            "branch": git("branch", "--show-current"),
            "dirty": bool(git("status", "--porcelain")),
        }
    except (OSError, subprocess.CalledProcessError):
        identity = {"commit": None, "branch": None, "dirty": None}
    identity["source_sha256"] = {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*.py"))
        if ".git" not in p.parts
        and "Results" not in p.parts
        and "tests" not in p.parts
        and ".superpowers" not in p.parts
    }
    return identity


def _json(path, value):
    """有限值与稳定键序，拒绝将 NaN 冒充有效统计。"""
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )


def _write_chain(chain, filename, scope, confirmation=None):
    """沿用原 Block repr/树文本；明确 ASCII 只覆盖主父投影。"""
    path = global_var.get_chain_data_path() / filename
    with path.open("a", encoding="utf-8") as f:
        print(
            f"Conflux scope: {scope}; ASCII main-parent tree projection, reference edges in dag_snapshot.json",
            file=f,
        )
        if chain.protocol_view:
            print(
                "Current pivot:",
                [chain.block_set[h].name for h in chain.protocol_view.pivot],
                file=f,
            )
            print(
                "Current provisional order:",
                [chain.block_set[h].name for h in chain.protocol_view.order],
                file=f,
            )
        # 全局观察图不提供共识确认；本地文本追加政策和序。
        if confirmation is not None:
            print(
                f"Confirmation policy: {confirmation.policy}; N={confirmation.N}; reversible empirical policy",
                file=f,
            )
            print(
                "Current confirmed order:",
                [chain.block_set[h].name for h in confirmation.current_order],
                file=f,
            )
    chain.printchain2txt(filename)


def _draw_graph(chain, *, filename, scope, miner_id=None):
    """共用只读渲染器，明确全局/本地图范围。"""
    from util.dag_plot import draw_dag_graph

    return draw_dag_graph(
        chain,
        global_var.get_chain_data_path(),
        filename=filename,
        scope=scope,
        miner_id=miner_id,
        show=global_var.get_show_fig(),
    )


def write_results(env, stats):
    """核心 manifest/metrics/txt 保留；非compact才输出逐节点全图/dump/事件/图。

    DAG dump 仅为可信本地 accepted Chain 快照，缺少 pending/tape/network，不是运行检查点。
    dag_events记录状态差量；完整dag_inputs用于重验输入，不充当整场检查点。
    """
    # 按实际本次run写结构化文件，manifest摘要防止混入旧文件。
    started = time.perf_counter()
    root, chains = global_var.get_result_path(), global_var.get_chain_data_path()
    # 独立展示参数，不改统计reference_miner_id。
    plot_miner_id = env.dag_settings.get("plot_miner_id", 0)
    _json(root / "dag_metrics.json", stats)
    _write_chain(
        env.global_chain,
        "chain_data.txt",
        "generated observer graph (not a public consensus view)",
    )
    with (root / "evaluation results.txt").open("a", encoding="utf-8") as f:
        for key, value in stats.items():
            # 原结果文本继续输出明确的数据项单位。
            unit = (
                " items/round"
                if key.startswith("dataitem_throughput")
                or key in ("valid_dataitem_throughput", "input_dataitem_rate")
                else (
                    " blocks/round"
                    if key.startswith("block_throughput")
                    else (
                        " MB/round"
                        if key.startswith("throughput_")
                        else (
                            " rounds/block"
                            if key.startswith("average_block_time")
                            else ""
                        )
                    )
                )
            )
            print(f"{key}: {value}{unit}", file=f)
    manifest = {  # 原五网络/双图继续有效，增加抽象数据项能力标识。
        "schema_version": "chainxim_conflux_run_r11_v1",
        "profile": "honest_conflux_pow",
        "miner_num": env.miner_num,
        "source": _git_identity(),
        "python": platform.python_version(),
        "reference_miner_id": env.reference_miner_id,
        "consensus_type": global_var.get_consensus_type(),
        "network_type": global_var.get_network_type(),
        "consensus_param": env.consensus_param,
        "network_param": env.network_param,
        "dataitem_param": env.dataitem_params,
        "dag_settings": env.dag_settings,
        "size_model": env.dag_settings["size_model"],
        "blocksize_MB": global_var.get_blocksize(),
        "requested_mining_rounds": env.requested_rounds,
        "actual_mining_rounds": env.total_round,
        "requested_max_pivot_height": env.requested_max_height,
        "drain_rounds_executed": env.drain_rounds_executed,
        "stop_reason": env.stop_reason,
        "output_settings": {
            "compact_outputfile": global_var.get_compact_outputfile(),
            "show_fig": global_var.get_show_fig(),
            "log_level": global_var.get_log_level(),
            # 记录实际绘图节点及文件范围。
            "plot_miner_id": plot_miner_id,
            "dag_graphs": (
                {}
                if global_var.get_compact_outputfile()
                else {
                    "global_generated": "Chain Data/dag_structure.svg",
                    "node_accepted": f"Chain Data/dag_structure_node{plot_miner_id}.svg",
                }
            ),
        },
        "inactive_parameters": {
            "consensus_miner_quantile": env.consensus_miner_quantile
        },
        # 实際有效模型与免费控制假设分别输出。
        "network_assumptions": [
            (
                "original topology bandwidth/delay/outage with full DAG past synchronization"
                if env.network.withTopology
                else "original abstract complete-block propagation without bandwidth simulation"
            ),
            "wire arrival is not dependency-closed acceptance",
            "original free-control-message assumption retained",
        ],
        "limitations": [
            "pivot_depth is reversible empirical confirmation, without deterministic finality or paper risk probability",
            "replay verifies each node consensus input state; not an in-flight network or PoW execution checkpoint",
            "SHA256 PoW deterministic; seed only controls actual random/NumPy users",
        ],
    }
    # 原N只在none下标为inactive，实验开关不另建入口。
    policy = env.dag_settings["confirmation_policy"]
    manifest["confirmation_policy"] = dict(
        name=policy,
        N=env.confirm_delay,
        enabled=policy != "none",
        reversible=True,
        model="empirical pivot_depth",
        risk_estimate=None,
        genesis_counted=False,
        drain_clock_adds_depth=False,
        late_premined_blocks_can_change_local_confirmation=True,
    )
    if policy == "none":
        manifest["inactive_parameters"]["N"] = env.confirm_delay
    # interval0全知输入政策显式公开，不误称分布式交易池。
    manifest["dataitem_policy"] = {
        "active": env.dag_dataitems_active,
        "item_bytes": 8,
        "encoding": "random_id:u32be + input_round:u32be; opaque 8-byte identity",
        "input_mode": (
            (
                "global_generated_pool"
                if env.dataitem_params["dataitem_input_interval"] == 0
                else "periodic_local_queue"
            )
            if env.dag_dataitems_active
            else "disabled"
        ),
        "local_queue_enabled": env.dag_dataitems_active
        and env.dataitem_params["dataitem_input_interval"] > 0,
        "global_pool_replacement": "previous mining round unique generated-block contents, regardless of order/confirmation",
        "completion": "unique current provisional order; not confirmation",
        "queue_retention": "all locally known unordered items; no silent capacity trimming",
    }

    # 在输出结束前保存尚未传播完成的路由；零区块时也建立空记录文件。
    if getattr(env.network, "_save_routing_history", False) and hasattr(
        env.network, "save_rest_routing_process"
    ):
        env.network.save_rest_routing_process()
    if global_var.get_compact_outputfile():
        _finish_manifest(env, manifest, root, started, full=False)
        return
    _json(
        chains / "dag_snapshot.json",
        {
            "schema_version": "chainxim_conflux_snapshot_r11_v1",
            "generated_graph_scope": "observer only; its protocol view is not public consensus",
            "generated_graph": graph_snapshot(env.global_chain),
            "nodes": [
                {
                    "miner_id": m.miner_id,
                    "accepted_graph": graph_snapshot(m.get_local_chain()),
                    "pending_hashes": sorted(
                        h.hex() for h in m.consensus.dependency_resolver.pending
                    ),
                    "wire_rounds": {h.hex(): v for h, v in m.dag_wire_rounds.items()},
                    "accepted_rounds": {
                        h.hex(): v for h, v in m.dag_accepted_rounds.items()
                    },
                    # 同一节点数据项状态与accepted/order快照对应。
                    "dataitems": (
                        m.dag_dataitems.snapshot()
                        if m.dag_dataitems is not None
                        else None
                    ),
                    # 当前和历史首次确认分别保存，hex仅在输出边界。
                    "confirmation": m.consensus.confirmation_tracker.snapshot(),
                    "dataitem_first_confirmed": {
                        x.hex(): dict(v)
                        for x, v in m.dag_dataitem_first_confirmed.items()
                    },
                    "dataitem_confirmation_events": m.dag_dataitem_confirmation_events,
                    "state_history": m.dag_state_history,
                }
                for m in env.miners
            ],
        },
    )
    for miner in env.miners:
        _write_chain(
            miner.get_local_chain(),
            f"chain_data{miner.miner_id}.txt",
            f"node {miner.miner_id} accepted graph",
            confirmation=miner.consensus.confirmation_tracker,
        )
    # 保留原dump路径，DAG用安全JSON；单链pickle不变。
    from .dag_replay import settings_from_manifest
    from .dag_audit import node_events

    env.local_chain_tracker.dump_dag_events(
        chains / "chain_dump.bin",
        [m.get_local_chain() for m in env.miners],
        settings_from_manifest(manifest),
    )
    _json(
        chains / "dag_edges.json",
        dict(
            schema_version="chainxim_conflux_edges_r11_v1",
            direction="child -> dependency",
            generated_graph=_edges(env.global_chain),
            nodes=[
                dict(miner_id=m.miner_id, edges=_edges(m.get_local_chain()))
                for m in env.miners
            ],
        ),
    )
    # 保持状态事件格式兼容，完整输入另存dag_inputs。
    with (chains / "dag_events.jsonl").open("w", encoding="utf-8") as f:
        for miner in env.miners:
            events = [
                ("protocol_delta_observation", e) for e in miner.dag_protocol_events
            ]
            events += [
                ("confirmation_delta_observation", e)
                for e in miner.consensus.confirmation_tracker.events
            ]
            events += [
                ("dataitem_confirmation_observation", e)
                for e in miner.dag_dataitem_confirmation_events
            ]
            for kind, event in sorted(
                events, key=lambda pair: (pair[1]["sequence"], pair[0])
            ):
                f.write(
                    json.dumps(
                        dict(
                            schema_version="chainxim_conflux_state_event_r9_v1",
                            kind=kind,
                            miner_id=miner.miner_id,
                            **event,
                        ),
                        sort_keys=True,
                        allow_nan=False,
                    )
                    + "\n"
                )
    with (chains / "dag_inputs.jsonl").open("w", encoding="utf-8") as f:
        for miner in env.miners:
            for event in node_events(miner):
                f.write(json.dumps(event, sort_keys=True, allow_nan=False) + "\n")
    # 默认图恢复全局结构口径；另保存指定节点本地图。
    _draw_graph(env.global_chain, filename="dag_structure", scope="global_generated")
    _draw_graph(
        env.miners[plot_miner_id].get_local_chain(),
        filename=f"dag_structure_node{plot_miner_id}",
        scope="node_accepted",
        miner_id=plot_miner_id,
    )
    # 原Topology详细路由图开关继续有效。
    if getattr(env.network, "_save_routing_graph", False) and hasattr(
        env.network, "gen_routing_gragh_from_json"
    ):
        env.network.gen_routing_gragh_from_json()

    _finish_manifest(env, manifest, root, started, full=True)


def _edges(chain):
    """每块直接main/ref边列表，不把传递祖先写入头。"""
    edges = []
    for h, b in sorted(chain.block_set.items()):
        if b.isGenesis:
            continue
        edges.append(
            dict(child=h.hex(), parent=b.blockhead.prehash.hex(), role="main_parent")
        )
        edges.extend(
            dict(child=h.hex(), parent=r.hex(), role="reference")
            for r in b.blockhead.refhashes
        )
    return edges


def _finish_manifest(env, manifest, root, started, full):
    """真实工程口径及本次结构化文件摘要；不用追加txt解析。"""
    names = ["dag_metrics.json"]
    if full:
        names += [
            "Chain Data/dag_snapshot.json",
            "Chain Data/chain_dump.bin",
            "Chain Data/dag_events.jsonl",
            "Chain Data/dag_inputs.jsonl",
            "Chain Data/dag_edges.json",
        ]
    manifest["artifact_sha256"] = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names
    }
    peak = None
    try:
        import resource

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if platform.system() != "Darwin":
            peak *= 1024
    except ImportError:
        pass
    manifest["engineering"] = dict(
        execution_seconds=getattr(env, "dag_execution_seconds", None),
        output_seconds=time.perf_counter() - started,
        protocol_seconds=sum(m.consensus.audit_protocol_seconds for m in env.miners),
        resolver_seconds=sum(m.consensus.audit_ingest_seconds for m in env.miners),
        process_peak_rss_bytes=peak,
        scope="execution/output elapsed; RSS is process-lifetime high-water, not isolated run peak",
    )
    manifest["input_audit"] = dict(
        enabled=full,
        schema_version="chainxim_conflux_input_event_r11_v1",
        scope="each NIC node actual complete block batches/self generation and item inputs",
        replay_entry="python -m util.dag_replay --result_path <run> --replay",
        accepted_dump="safe JSON envelope; accepted-only; not checkpoint",
    )
    _json(root / "run_manifest.json", manifest)
