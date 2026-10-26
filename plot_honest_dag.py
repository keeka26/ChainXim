"""全诚实节点统计分析：只读取真实结果，不运行仿真。

python plot_honest_dag.py --results-root Results/honest_smoke --case all
支持analysis_grid.json或直接声明轮数/固定参数/扫描轴；原生终端命令见
simulation-example.dag.zh.md。绘图不生成逐点INI，也不启动仿真。
Bitcoin=本仓库consensus.PoW单链模型，DAG=ConfluxPoW。
默认对照Bitcoin生成/原生主链与DAG生成/共同序；区块大小组仅对照M_B与G_D。
三类统计分别为总生成、可利用吞吐代理和当前排序进度；DAG前两类均为G_D/T。
G_D/T以合法块最终可被纳入为假设，不表示实测排序或确认；共同序也不代表终局。
新结果直接读取dag_metrics.json中的共同前缀计数，compact也支持共同序曲线。
"""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import reprlib
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

NETWORKS = (
    "LockstepNetwork",
    "DeterPropNetwork",
    "StochPropNetwork",
    "TopologyNetwork",
    "AdHocNetwork",
)
CASES = ("miners", "hashrate", "difficulty", "blocksize")
PROTOCOLS = ("Bitcoin", "DAG")
SCHEMA = "chainxim_honest_analysis_grid_v1"


def read_json(path):
    """严格读取有限JSON，拒绝重复键和NaN，避免静默覆盖分析输入。"""

    def pairs(items):
        result = {}
        for k, v in items:
            if k in result:
                raise ValueError("duplicate JSON key: " + k)
            result[k] = v
        return result

    def number(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("nonfinite JSON number")
        return result

    def bad(value):
        raise ValueError("nonfinite JSON constant: " + value)

    return json.loads(
        Path(path).read_text(encoding="utf-8"),
        object_pairs_hook=pairs,
        parse_float=number,
        parse_constant=bad,
    )


def numeric(value, name, *, minimum=0, integer=False):
    """规范数值边界；bool不充当节点数，非有限值不进入图。"""
    if type(value) not in (int, float) or not math.isfinite(value) or value < minimum:
        raise ValueError(f"invalid {name}: {value!r}")
    if integer and type(value) is not int:
        raise ValueError(name + " must be int")
    return value


def load_grid(path):
    """读取声明的参数矩阵；它只描述实验，不触发仿真。"""
    return validate_grid(read_json(path))


def validate_grid(grid):
    """文件与直接CLI共用相同的参数边界检查。"""
    if grid.get("schema_version") != SCHEMA:
        raise ValueError("unsupported analysis grid schema")
    for key in ("rounds", "miners", "q", "difficulty", "blocksize"):
        value = grid["fixed"][key]
        numeric(
            value,
            key,
            minimum=1 if key in ("rounds", "miners") else 0,
            integer=key != "blocksize",
        )
    for case in CASES:
        axis = grid["axes"][case]
        if not isinstance(axis, list) or not axis or len(set(axis)) != len(axis):
            raise ValueError("empty/duplicate axis: " + case)
        for x in axis:
            numeric(
                x,
                case,
                minimum=1 if case == "miners" else 0,
                integer=case != "blocksize",
            )
    for key, allowed in (("networks", NETWORKS), ("losses", ("low", "medium", "high"))):
        values = grid[key]
        if (
            not isinstance(values, list)
            or not values
            or len(set(values)) != len(values)
            or any(v not in allowed for v in values)
        ):
            raise ValueError("invalid " + key)
    if not grid["seeds"] or len(set(grid["seeds"])) != len(grid["seeds"]):
        raise ValueError("empty/duplicate seeds")
    for seed in grid["seeds"]:
        numeric(seed, "seed", integer=True)
        if seed >= 2**32:
            raise ValueError("seed out of uint32 range")
    return grid


def command_grid(options):
    """从绘图命令构造预期矩阵，不创建任何配置文件。

    五个固定参数必须明示，当前case的扫描轴必须明示；其余轴只放固定值。
    因此strict仍能检测整个横轴点缺失，不能按现存结果反推一个不完整矩阵。
    """
    fixed = {}
    for key in ("rounds", "miners", "q", "difficulty", "blocksize"):
        if options.get(key) is None:
            raise ValueError("CLI matrix requires --" + key)
        fixed[key] = options[key]
    cases = (
        CASES
        if options["case"] == "all"
        else ("miners",) if options["case"] == "dag" else (options["case"],)
    )
    axes = {}
    for case, key in (
        ("miners", "miners"),
        ("hashrate", "q"),
        ("difficulty", "difficulty"),
        ("blocksize", "blocksize"),
    ):
        axis = options.get(case + "_axis")
        if case in cases and axis is None:
            raise ValueError("CLI matrix requires --" + case + "-axis")
        axes[case] = axis if axis is not None else [fixed[key]]
    return validate_grid(
        dict(
            schema_version=SCHEMA,
            fixed=fixed,
            axes=axes,
            networks=options.get("networks") or list(NETWORKS),
            seeds=options.get("seeds") or [7],
            losses=options.get("losses") or ["low", "medium", "high"],
        )
    )


def axis_number(value):
    """保留块大小轴的整数/小数身份，使2→b2、2.0→b2.0。"""
    return int(value) if re.fullmatch(r"[+-]?\d+", value) else float(value)


def expected_points(grid, cases):
    """约定目录与横轴含义；一份矩阵对应一个结果根，不混合其他扫描。"""
    out = []
    for case in cases:
        nets = (
            grid["networks"]
            if case == "miners"
            else [
                (
                    "DeterPropNetwork"
                    if case == "hashrate"
                    else "TopologyNetwork" if case == "difficulty" else "AdHocNetwork"
                )
            ]
        )
        losses = grid["losses"] if case == "blocksize" else ["none"]
        for protocol in PROTOCOLS:
            for net in nets:
                for loss in losses:
                    for x in sorted(grid["axes"][case]):
                        values = dict(grid["fixed"])
                        values[
                            {
                                "miners": "miners",
                                "hashrate": "q",
                                "difficulty": "difficulty",
                                "blocksize": "blocksize",
                            }[case]
                        ] = x
                        for seed in grid["seeds"]:
                            token = {
                                "miners": "n",
                                "hashrate": "q",
                                "difficulty": "d",
                                "blocksize": "b",
                            }[case] + str(x)
                            parts = [case, protocol, net]
                            if loss != "none":
                                parts.append(loss)
                            parts.extend([token, "seed" + str(seed)])
                            out.append(
                                dict(
                                    case=case,
                                    protocol=protocol,
                                    network=net,
                                    loss=loss,
                                    x=x,
                                    seed=seed,
                                    path="/".join(parts),
                                    **values,
                                )
                            )
    return out


def expected_rate(miners, q, difficulty):
    """理想均匀SHA下每节点成功即停的生成率；不是有效账本吞吐保证。"""
    p = ((2**256 - 1) >> difficulty) / 2**256
    if p == 0 or q == 0:
        return 0.0
    if p >= 1:
        return float(miners)
    return miners * (-math.expm1(math.ceil(q) * math.log1p(-p)))


def selected_points(grid, case):
    """DAG独立分析只要求DAG输入；其他case维持两协议对照。"""
    cases = list(CASES) if case == "all" else ["miners"] if case == "dag" else [case]
    points = expected_points(grid, cases)
    return [p for p in points if p["protocol"] == "DAG"] if case == "dag" else points


def ratio(a, b):
    """分母为空不制造零率；保留None供CSV和绘图按缺样本处理。"""
    return a / b if b else None


def derive_dag(metrics, snapshot=None):
    """结构计数取参考节点且排除创世；wire延迟覆盖全网非生产者接收配对。"""
    a, o, p = (metrics[k] for k in ("accepted", "ordered", "pivot_height"))
    for key, value in (("accepted", a), ("ordered", o), ("pivot_height", p)):
        numeric(value, key, integer=True)
    if not p <= o <= a:
        raise ValueError("invalid pivot/ordered/accepted counts")
    sizes = metrics.get("epoch_sizes", [])
    ends = {
        e["round"]: e["tips"]
        for e in metrics.get("tips_time_series", [])
        if e["phase"] == "receive_end_round"
        and 1 <= e["round"] <= metrics["total_round"]
    }
    complete = len(ends) == metrics["total_round"] and bool(ends)
    node = next(
        (
            n
            for n in metrics.get("nodes", [])
            if n["miner_id"] == metrics.get("reference_miner_id", 0)
        ),
        {},
    )
    wire = metrics.get("wire_propagation", {})
    accepted = metrics.get("accepted_propagation", {})
    out = dict(
        side_fraction=ratio(a - p, a),
        ordered_side_fraction=ratio(o - p, o),
        reference_miner_id=metrics.get("reference_miner_id", 0),
        ordered_coverage=ratio(o, a),
        references_per_block=ratio(metrics["reference_count"], a),
        mean_epoch_size=sum(sizes) / len(sizes) if sizes else None,
        max_epoch_size=max(sizes) if sizes else None,
        tips_end=metrics.get("tips"),
        mean_round_end_tips=sum(ends.values()) / len(ends) if complete else None,
        peak_round_end_tips=max(ends.values()) if complete else None,
        tip_round_samples=len(ends),
        unassigned=metrics.get("unassigned_epoch_blocks", a - o),
        accepted=a,
        ordered=o,
        confirmed=metrics.get("confirmed"),
        pivot_height=p,
        max_dag_rank=metrics.get("max_dag_rank"),
        pending=metrics.get("pending"),
        order_removed_per_ordered=ratio(node.get("order_removed_blocks", 0), o),
        wire_mean_rounds=wire.get("mean_rounds"),
        wire_completed=wire.get("completed"),
        wire_unfinished=wire.get("unfinished"),
        accepted_mean_rounds=accepted.get("mean_rounds"),
        accepted_unfinished=accepted.get("unfinished"),
        unfinished_network_packets=metrics.get("unfinished_network_packets"),
        local_graphs_equal=metrics.get("local_graphs_equal"),
        local_views_equal=metrics.get("local_views_equal"),
        multi_parent_fraction=None,
        generated_side_fraction=None,
    )
    if snapshot:
        graph = snapshot["nodes"][metrics["reference_miner_id"]]["accepted_graph"]
        blocks = [b for b in graph["blocks"] if not b["isGenesis"]]
        out["multi_parent_fraction"] = ratio(
            sum(bool(b["refhashes"]) for b in blocks), len(blocks)
        )
        global_graph = snapshot["generated_graph"]
        g = len(global_graph["blocks"]) - 1
        out["generated_side_fraction"] = ratio(g - (len(global_graph["pivot"]) - 1), g)
    return out


def unique_line(text, key, *, required=True):
    """原txt为追加格式；同一关键字段出现两次时拒绝整个点。"""
    rows = re.findall(r"^" + re.escape(key) + r":\s*([^\n]*)", text, re.M)
    if len(rows) > 1:
        raise ValueError("duplicate metric/parameter: " + key)
    if not rows:
        if required:
            raise ValueError("missing metric/parameter: " + key)
        return None
    return rows[0].strip()


def text_number(text, key):
    """读取带原单位后缀的数值，不从None等字符串中挖取伪数值。"""
    value = unique_line(text, key)
    match = re.fullmatch(
        r"([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)(?:\s+[A-Za-z/_]+)?", value
    )
    if not match:
        raise ValueError("not numeric: " + key)
    return numeric(float(match[1]), key)


def display_network(params):
    """保留完整参数；校验不能使用reprlib截断后的展示值。"""
    return dict(params)


def legacy_network(folder, params):
    """恢复旧txt的列表；DeterProp优先核对实际网络日志，拒绝丢失尾部。"""
    out = dict(params)
    for key in ("stat_prop_times", "prop_vector"):
        if key not in out:
            continue
        shown = out[key]
        attrs = folder / "Network Results" / "network_attributes.txt"
        if key == "prop_vector" and attrs.exists():
            value = ast.literal_eval(
                unique_line(attrs.read_text(encoding="utf-8"), "propagation_vector")
            )
            if reprlib.repr(value) != shown:
                raise ValueError("propagation vector evidence mismatch")
        elif key == "stat_prop_times":
            # 原生长阈值列表会被parameters截断；原evaluation的
            # 完整传播统计字典保留全部阈值键，用实际输出恢复，不读取/猜测基础INI。
            evaluation = folder / "evaluation results.txt"
            evidence = (
                unique_line(
                    evaluation.read_text(encoding="utf-8"),
                    "block_propagation_times",
                    required=False,
                )
                if evaluation.exists()
                else None
            )
            if evidence is None:
                value = ast.literal_eval(shown) if isinstance(shown, str) else shown
            else:
                times = ast.literal_eval(evidence)
                if not isinstance(times, dict):
                    raise ValueError("invalid propagation statistics evidence")
                value = list(times)
                if reprlib.repr(value) != shown:
                    raise ValueError("statistical thresholds evidence mismatch")
        else:
            value = ast.literal_eval(shown) if isinstance(shown, str) else shown
        if not isinstance(value, list) or not value:
            raise ValueError("invalid network vector: " + key)
        for v in value:
            numeric(v, key)
        out[key] = value
    return out


def check_budget(actual, expected):
    """挖矿循环采用ceil(q)，因此整数预算必须精确匹配，不能容忍近整数。"""
    numeric(actual, "q")
    if actual != expected or actual != math.ceil(actual):
        raise ValueError("integer hash budget mismatch")


def legacy_target(folder, expected, generated):
    """用全局块头核对完整目标及块数；零产块时如实标记无普通块证据。"""
    path = folder / "Chain Data" / "chain_data.txt"
    if not path.exists() and generated == 0:
        return False
    text = path.read_text(encoding="utf-8")
    parts = re.split(
        r"^\s*__blockhead:\s*target:\s*([0-9a-fA-F]{64})\s*$", text, flags=re.M
    )
    count = 0
    for target, body in zip(parts[1::2], parts[2::2]):
        genesis = re.findall(r"^\s*isGenesis:\s*(True|False)\s*$", body, re.M)
        if len(genesis) != 1:
            raise ValueError("ambiguous legacy block header")
        if genesis[0] == "True":
            continue
        count += 1
        if target.upper() != expected:
            raise ValueError("full block target/difficulty mismatch")
    if count != generated:
        raise ValueError("global block dump count mismatch")
    return generated > 0


def check_close(actual, expected, name):
    """计数/种子精确相等；其他浮点只容许输出舍入误差。"""
    same = (
        actual == expected
        if type(expected) is int
        else math.isclose(float(actual), float(expected), rel_tol=1e-6, abs_tol=1e-8)
    )
    if type(actual) is bool or not same:
        raise ValueError(f"{name}: actual {actual!r} != expected {expected!r}")


def common_order_count(snapshot):
    """从已由reader核验的旧完整快照求全节点全序LCP，排除创世。

    只能比较相同位置的hash；共同块集合和最短序列长度都不能替代共同前缀。
    缺快照返回None，调用者据此保留缺值，不能回退到参考节点排序量。
    """
    if snapshot is None:
        return None
    from util.dag_analysis import common_order_length

    return common_order_length(
        node["accepted_graph"]["order"] for node in snapshot["nodes"]
    )


def read_point(folder, point):
    """读一次真实运行；DAG复用安全reader，单链严格读取原txt。"""
    folder = Path(folder)
    row = dict(point)
    target = f"{((2**256-1)>>point['difficulty']):064X}"
    if point["protocol"] == "DAG":
        from util.dag_replay import read_results

        result = read_results(folder, replay=False)
        manifest, m = result["manifest"], result["metrics"]
        if manifest["consensus_type"] != "consensus.ConfluxPoW":
            raise ValueError("expected ConfluxPoW")
        if manifest["network_type"] != "network." + point["network"]:
            raise ValueError("network mismatch")
        c = manifest["consensus_param"]
        if c["target"].upper() != target:
            raise ValueError("target/difficulty mismatch")
        if manifest["dag_settings"]["size_model"] != "fixed_total":
            raise ValueError("comparison requires fixed_total")
        if manifest["dag_settings"]["drain_rounds"] != 0:
            raise ValueError("comparison requires drain_rounds=0")
        if manifest["dataitem_policy"]["active"]:
            raise ValueError("comparison requires disabled dataitems")
        attack = manifest.get("attack_param", {})
        if attack.get("adversary_ids") or manifest.get("attack_policy"):
            raise ValueError("not an honest run")
        check_close(manifest["miner_num"], point["miners"], "miners")
        check_close(manifest["dag_settings"]["seed"], point["seed"], "seed")
        check_close(manifest["blocksize_MB"], point["blocksize"], "blocksize")
        check_budget(c["q_ave"], point["q"])
        if c["q_distr"] != "equal":
            raise ValueError("comparison requires equal hash budgets")
        g, t = m["generated"], m["total_round"]
        check_close(g / t if t else 0, m["block_throughput_total"], "total throughput")
        check_close(
            m["throughput_total_MB"], g * point["blocksize"] / t if t else 0, "total MB"
        )
        # 原参考节点指标继续核对和保留，主对照改为全部节点的共同序。
        reference_ordered = m["ordered_at_mining_end"]
        check_close(
            m["block_throughput_ordered"],
            reference_ordered / t if t else 0,
            "ordered throughput",
        )
        # 新增：优先使用运行结束时已冻结的LD，compact不再需要完整图。
        # 旧完整结果仍从快照计算；旧compact缺值不以参考节点数量回填。
        if "common_order_at_mining_end" in m:
            ledger = m["common_order_at_mining_end"]
            common_source = "dag_metrics"
        else:
            ledger = common_order_count(result["snapshot"])
            common_source = "dag_snapshot" if ledger is not None else None
        row.update(
            ledger_kind="dag_all_node_common_order_at_mining_end",
            common_order_blocks=ledger,
            common_order_source=common_source,
            reference_ordered_at_mining_end=reference_ordered,
            reference_ordered_blocks_per_round=reference_ordered / t if t else None,
            reference_ordered_MB_per_round=(
                reference_ordered * point["blocksize"] / t if t else None
            ),
        )
        row.update(derive_dag(m, result["snapshot"]))
        network = display_network(manifest["network_param"])
        row.update(
            seed_verified=True,
            target_verified=True,
            target_source="run_manifest",
            _snapshot=result["snapshot"],
            _metrics=m,
        )
        inputs = ["run_manifest.json", "dag_metrics.json"]
        if result["snapshot"] is not None:
            inputs.append("Chain Data/dag_snapshot.json")
    else:
        ptext = (folder / "parameters.txt").read_text(encoding="utf-8")
        text = (folder / "evaluation results.txt").read_text(encoding="utf-8")
        if unique_line(ptext, "Consensus Protocol") != "PoW":
            raise ValueError("expected Bitcoin PoW")
        if unique_line(ptext, "Network Type") != point["network"]:
            raise ValueError("network mismatch")
        if unique_line(ptext, "Adversary Miners", required=False):
            raise ValueError("not an honest run")
        if "Dataitem Param:" in ptext or re.search(
            r"^input_dataitem_rate:", text, re.M
        ):
            raise ValueError("comparison requires disabled dataitems")
        check_close(text_number(ptext, "Miner Number"), point["miners"], "miners")
        c = ast.literal_eval(unique_line(ptext, "Consensus Param"))
        if c["target"].upper() != reprlib.repr(target).strip("'").upper():
            raise ValueError("target/difficulty mismatch")
        check_budget(c["q_ave"], point["q"])
        if c["q_distr"] != "equal":
            raise ValueError("comparison requires equal hash budgets")
        b = unique_line(ptext, "Block Size", required=False)
        if b is not None:
            check_close(float(b), point["blocksize"], "blocksize")
        network = legacy_network(
            folder, ast.literal_eval(unique_line(ptext, "Network Param"))
        )
        g, t = text_number(text, "num_of_generated_blocks"), text_number(
            text, "total_round"
        )
        if not g.is_integer() or not t.is_integer():
            raise ValueError("counts/rounds must be integers")
        g, t = int(g), int(t)
        check_close(
            text_number(text, "block_throughput_total"),
            g / t if t else 0,
            "total throughput",
        )
        check_close(
            text_number(text, "throughput_total_MB"),
            g * point["blocksize"] / t if t else 0,
            "total MB",
        )
        # 沿用原单链主链统计，不以全局最大高度替换其统计口径。
        ledger = text_number(text, "num_of_valid_blocks")
        if not ledger.is_integer():
            raise ValueError("main-chain count must be an integer")
        ledger = int(ledger)
        check_close(
            text_number(text, "block_throughput_main"),
            ledger / t if t else 0,
            "main throughput",
        )
        row["ledger_kind"] = "bitcoin_original_main_chain"
        # 旧parameters不记录seed；目录标签不能伪装成已核验随机源。
        verified = legacy_target(folder, target, g)
        row.update(
            seed_verified=False,
            target_verified=verified,
            target_source=(
                "ordinary_block_headers"
                if verified
                else "truncated_parameters_only_zero_blocks"
            ),
            _snapshot=None,
            _metrics=None,
        )
        inputs = ["parameters.txt", "evaluation results.txt"]
        for name in (
            "Chain Data/chain_data.txt",
            "Network Results/network_attributes.txt",
        ):
            if (folder / name).exists():
                inputs.append(name)
    check_close(t, point["rounds"], "actual mining rounds")
    if not t:
        raise ValueError("analysis requires positive mining rounds")
    numeric(g, "generated", integer=True)
    if ledger is not None:
        numeric(ledger, "ledger_blocks", integer=True)
        if ledger > g:
            raise ValueError("main/common-order count exceeds generated count")
    if point["case"] == "blocksize":
        if network.get("enable_large_scale_fading") is not True:
            raise ValueError("path-loss comparison requires fading=True")
        if network.get("path_loss_level") != point["loss"]:
            raise ValueError("path-loss mismatch")
    row.update(
        generated=g,
        total_round=t,
        total_blocks_per_round=g / t,
        total_MB_per_round=g * point["blocksize"] / t,
        ledger_blocks=ledger,
        ledger_blocks_per_round=ledger / t if ledger is not None else None,
        ledger_MB_per_round=(
            ledger * point["blocksize"] / t if ledger is not None else None
        ),
        # 可利用吞吐代理：Bitcoin保留原主链，DAG假设全部合法生成块最终纳入。
        eventual_kind=(
            "dag_generation_assuming_eventual_inclusion"
            if point["protocol"] == "DAG"
            else "bitcoin_original_main_chain"
        ),
        eventual_blocks=g if point["protocol"] == "DAG" else ledger,
        eventual_blocks_per_round=(g if point["protocol"] == "DAG" else ledger) / t,
        eventual_MB_per_round=(g if point["protocol"] == "DAG" else ledger)
        * point["blocksize"]
        / t,
        expected_blocks_per_round=expected_rate(
            point["miners"], point["q"], point["difficulty"]
        ),
        _network=network,
        _input_sha256={
            name: hashlib.sha256((folder / name).read_bytes()).hexdigest()
            for name in inputs
        },
    )
    return row


def aggregate_series(rows, case, network, protocol, loss, metric, xs):
    """同一点按seed等权均值和min/max；缺失用NaN断线，不插值或补零。"""
    selected = [
        r
        for r in rows
        if (r["case"], r["network"], r["protocol"], r["loss"])
        == (case, network, protocol, loss)
    ]
    keys = [(r["x"], r["seed"]) for r in selected]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate seed result for one point")
    out = dict(x=list(xs), mean=[], low=[], high=[], runs=[])
    for x in xs:
        values = [r.get(metric) for r in selected if r["x"] == x]
        values = [v for v in values if v is not None and math.isfinite(v)]
        out["runs"].append(len(values))
        out["mean"].append(sum(values) / len(values) if values else math.nan)
        out["low"].append(min(values) if values else math.nan)
        out["high"].append(max(values) if values else math.nan)
    return out


def validate_pairs(rows):
    """同横轴、同seed的两协议必须共享网络设置；不把种子当共同随机轨迹。"""
    groups = {}
    for row in rows:
        key = tuple(row[k] for k in ("case", "network", "loss", "x", "seed"))
        groups.setdefault(key, {})[row["protocol"]] = row
    for key, pair in groups.items():
        if (
            set(pair) == set(PROTOCOLS)
            and pair["Bitcoin"]["_network"] != pair["DAG"]["_network"]
        ):
            raise ValueError("paired network settings differ: " + str(key))
    # 控制扫描内传播参数；miners允许随n改变拓扑文件与相应度数。
    settings = {}
    for row in rows:
        key = tuple(row[k] for k in ("case", "network"))
        current = dict(row["_network"])
        if row["case"] == "blocksize":
            current.pop("path_loss_level", None)
        if row["case"] == "miners":
            current.pop("topology_path", None)
            current.pop("ave_degree", None)
        if key in settings and current != settings[key]:
            raise ValueError(
                "network settings changed during controlled sweep: " + str(key)
            )
        settings[key] = current


def save_figure(fig, out, name):
    """保存可分享PNG与矢量PDF；仅写绘图输出，不碰原运行文件。"""
    fig.tight_layout()
    files = []
    for suffix in ("png", "pdf"):
        path = out / (name + "." + suffix)
        fig.savefig(path, dpi=220)
        files.append(str(path))
    plt.close(fig)
    return files


def draw_series(ax, values, label, color, marker, linestyle="-", hollow=False):
    """只连接相邻有效横轴；阴影是seed观察范围，不称置信区间。"""
    if not any(math.isfinite(v) for v in values["mean"]):
        return False
    ax.plot(
        values["x"],
        values["mean"],
        color=color,
        marker=marker,
        markerfacecolor="none" if hollow else color,
        label=label,
        linewidth=1.4,
        linestyle=linestyle,
    )
    if max(values["runs"], default=0) > 1:
        ax.fill_between(
            values["x"], values["low"], values["high"], color=color, alpha=0.13
        )
    return True


def comparison_mode(case, throughput):
    """区块大小默认只比较可利用吞吐；显式total/ledger/eventual仍按所选口径。"""
    return "eventual" if case == "blocksize" and throughput == "both" else throughput


def plot_comparisons(rows, grid, out, cases, theory=False, throughput="both"):
    """默认四曲线；AdHoc区块大小以M_B和G_D对照，三档合计六曲线。"""
    if any(
        row["case"] in cases
        and comparison_mode(row["case"], throughput) in ("both", "ledger")
        and row["protocol"] == "DAG"
        and row.get("ledger_blocks_per_round") is None
        for row in rows
    ):
        raise ValueError(
            "DAG common order requires common_order_at_mining_end in dag_metrics.json "
            "or a full snapshot; legacy compact results need rerunning or --throughput total/eventual"
        )
    files = []
    xlabels = dict(
        miners=r"Miners, $n$",
        hashrate="Hash budget, $q$\n(trials miner$^{-1}$ round$^{-1}$)",
        difficulty=r"PoW difficulty, $d$ (bits)",
        blocksize=r"Block size, $B$ (MB)",
    )
    for case in cases:
        if not any(row["case"] == case for row in rows):
            continue
        nets = (
            grid["networks"]
            if case == "miners"
            else [
                (
                    "DeterPropNetwork"
                    if case == "hashrate"
                    else "TopologyNetwork" if case == "difficulty" else "AdHocNetwork"
                )
            ]
        )
        losses = grid["losses"] if case == "blocksize" else ["none"]
        tail = "MB" if case == "blocksize" else "blocks"
        mode = comparison_mode(case, throughput)
        factor = "B" if case == "blocksize" else ""
        # 图例直接给出计数公式；颜色区分路损，线型/标记区分协议和统计量。
        series = (
            ("Bitcoin", "total", "G_B", ":", "o", True),
            ("Bitcoin", "ledger", "M_B", "-", "o", False),
            ("DAG", "total", "G_D", "-.", "s", True),
            ("DAG", "ledger", "L_D", "--", "s", False),
        )
        if mode == "eventual":
            series = (
                ("Bitcoin", "eventual", "M_B", "-", "o", False),
                ("DAG", "eventual", "G_D", "--", "s", False),
            )
        elif mode != "both":
            series = tuple(s for s in series if s[1] == mode)
        cols = min(3, len(nets))
        nr = math.ceil(len(nets) / cols)
        fig, axes = plt.subplots(
            nr,
            cols,
            figsize=(9.4, 5.8) if case == "blocksize" else (5.3 * cols, 4.1 * nr),
            squeeze=False,
        )
        for ax, net in zip(axes.flat, nets):
            for loss in losses:
                for protocol, scope, count, linestyle, marker, hollow in series:
                    color = {"Bitcoin": "#3667a6", "DAG": "#c86435"}[protocol]
                    legend = rf"{protocol}: ${factor}{count}/T$"
                    if case == "blocksize":
                        color = {
                            "low": "#3667a6",
                            "medium": "#c86435",
                            "high": "#3b8c5a",
                        }[loss]
                        legend += rf", $\ell=\mathrm{{{loss}}}$"
                    values = aggregate_series(
                        rows,
                        case,
                        net,
                        protocol,
                        loss,
                        scope + "_" + tail + "_per_round",
                        sorted(grid["axes"][case]),
                    )
                    draw_series(ax, values, legend, color, marker, linestyle, hollow)
            # 只参照生成率；共同序和原生主链没有套用这一理论线。
            if theory and mode in ("both", "total"):
                points = [
                    point
                    for point in expected_points(grid, [case])
                    if point["network"] == net
                    and point["loss"] == losses[0]
                    and point["protocol"] == "DAG"
                    and point["seed"] == grid["seeds"][0]
                ]
                ys = [
                    expected_rate(point["miners"], point["q"], point["difficulty"])
                    * (point["blocksize"] if case == "blocksize" else 1)
                    for point in points
                ]
                ax.plot(
                    [point["x"] for point in points],
                    ys,
                    ":",
                    color="#555555",
                    label=rf"${factor}\lambda_{{\mathrm{{gen}}}}$ (ideal)",
                )
            ax.set(
                xlabel=xlabels[case],
                ylabel=(
                    r"$\Theta_{\mathrm{MB}}$ (MB round$^{-1}$)"
                    if case == "blocksize"
                    else r"$\Theta$ (blocks round$^{-1}$)"
                ),
                title=net.removesuffix("Network"),
            )
            if len(grid["axes"][case]) <= 12:
                ax.set_xticks(sorted(grid["axes"][case]))
            ax.grid(alpha=0.25)
            ax.set_ylim(bottom=0)
            handles, _ = ax.get_legend_handles_labels()
            if handles:
                ax.legend(fontsize=8, ncol=3 if case == "blocksize" else 1)
        for ax in list(axes.flat)[len(nets) :]:
            ax.set_visible(False)
        variable = dict(miners="n", hashrate="q", difficulty="d", blocksize="B")[case]
        title = (
            rf"Nominal throughput $\Theta_{{\mathrm{{MB}}}}({variable})$"
            if case == "blocksize"
            else rf"Block throughput $\Theta({variable})$"
        )
        if mode == "eventual":
            title += "\nDAG: eventual-inclusion proxy"
        fig.suptitle(title, fontsize=12)
        name = (
            case
            + (
                "_throughput_comparison_"
                if throughput == "both"
                else (
                    "_ledger_throughput_"
                    if throughput == "ledger"
                    else (
                        "_eventual_throughput_"
                        if throughput == "eventual"
                        else "_throughput_"
                    )
                )
            )
            + tail
        )
        files += save_figure(fig, out, name)
    return files


def plot_dag_features(rows, grid, out):
    """复用miners扫描的DAG点，画并发与利用指标，不引入虚构分叉率。"""
    if not any(r["case"] == "miners" and r["protocol"] == "DAG" for r in rows):
        return []
    metrics = [
        ("side_fraction", "Local non-pivot fraction", r"$(A-P)/A$"),
        ("mean_round_end_tips", "Local frontier size", r"$\overline{F}$ (tips)"),
        (
            "references_per_block",
            "Local reference density",
            r"$R/A$ (edges block$^{-1}$)",
        ),
        ("mean_epoch_size", "Local epoch size", r"$O/P$ (blocks epoch$^{-1}$)"),
        ("ordered_coverage", "Local ordering coverage", r"$O/A$"),
        (
            "wire_mean_rounds",
            "Network propagation delay",
            r"$\overline{\Delta t}_{\mathrm{wire}}$ (rounds)",
        ),
    ]
    fig, axes = plt.subplots(2, 3, figsize=(15.2, 8))
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    for ax, (metric, title, label) in zip(axes.flat, metrics):
        for i, net in enumerate(grid["networks"]):
            v = aggregate_series(
                rows,
                "miners",
                net,
                "DAG",
                "none",
                metric,
                sorted(grid["axes"]["miners"]),
            )
            draw_series(
                ax, v, net.removesuffix("Network"), colors[i % len(colors)], "o"
            )
        ax.set(xlabel=r"Miners, $n$", ylabel=label, title=title)
        if len(grid["axes"]["miners"]) <= 12:
            ax.set_xticks(sorted(grid["axes"]["miners"]))
        ax.grid(alpha=0.25)
        ax.set_ylim(bottom=0)
        if metric in ("side_fraction", "ordered_coverage"):
            ax.set_ylim(0, 1)
        h, _ = ax.get_legend_handles_labels()
        if h:
            ax.legend(fontsize=7)
    fig.suptitle("DAG structure and propagation")
    return save_figure(fig, out, "dag_features")


def plot_trace(folder, out):
    """可选完整DAG结果：逐轮视角、epoch和引用分布；不从概要补造历史。"""
    from util.dag_replay import read_results

    result = read_results(folder, replay=False)
    if result["snapshot"] is None:
        raise ValueError("trace requires full DAG output")
    ref = result["metrics"]["reference_miner_id"]
    node = result["snapshot"]["nodes"][ref]
    history = [e for e in node["state_history"] if e["phase"] == "receive_end_round"]
    if not history:
        raise ValueError("trace has no round-end observations")
    graph = node["accepted_graph"]
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    for field, label in (
        ("accepted", "$A(t)$"),
        ("ordered", "$O(t)$"),
        ("confirmed", "$C(t)$"),
    ):
        values = [e.get(field) for e in history]
        if any(v is not None for v in values):
            axes[0, 0].plot([e["round"] for e in history], values, label=label)
    axes[0, 0].set(xlabel=r"Round, $t$", ylabel="Blocks", title="Local processing")
    axes[0, 0].legend()
    for field, label in (
        ("tips", "$F(t)$"),
        ("pivot_height", "$P(t)$"),
        ("max_dag_rank", r"$r_{\max}(t)$"),
    ):
        axes[0, 1].plot(
            [e["round"] for e in history], [e[field] for e in history], label=label
        )
    axes[0, 1].set(
        xlabel=r"Round, $t$", ylabel="Count / depth", title="Frontier and depth"
    )
    axes[0, 1].legend()
    sizes = [len(e) for e in graph["epochs"][1:]]
    axes[1, 0].bar(range(1, len(sizes) + 1), sizes)
    axes[1, 0].set(
        xlabel=r"Epoch, $e$", ylabel=r"$|E_e|$ (blocks)", title="Epoch sizes"
    )
    refs = [len(b["refhashes"]) for b in graph["blocks"] if not b["isGenesis"]]
    if refs:
        axes[1, 1].hist(refs, bins=np.arange(max(refs) + 2) - 0.5, rwidth=0.8)
    axes[1, 1].set(
        xlabel=r"References, $k$", ylabel="Blocks", title="Reference distribution"
    )
    for ax in axes.flat:
        ax.grid(alpha=0.2)
    fig.suptitle(
        rf"DAG local view: node $i={ref}$",
        fontsize=11,
    )
    return save_figure(fig, out, "dag_trace")


def export_rows(rows, out):
    """每个点保存可复核CSV；嵌套网络设置和输入摘要另写JSON。"""
    columns = sorted({k for row in rows for k in row if not k.startswith("_")})
    with (out / "observations.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in columns})


def main(argv=None):
    """原生离线CLI；失败点默认报错，缺失点记录并保持空缺。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-root", type=Path, required=True)
    parser.add_argument("--grid", type=Path, help="默认results-root/analysis_grid.json")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--case", choices=("all", *CASES, "dag"), default="all")
    parser.add_argument(
        "--theory", action="store_true", help="附加理想哈希预算期望，不替代实测"
    )
    # both默认四曲线，区块大小自动简化为eventual；数据仍保留全部统计口径。
    parser.add_argument(
        "--throughput",
        choices=("total", "ledger", "eventual", "both"),
        default="both",
        help="total=G_B/G_D，ledger=M_B/L_D，eventual=M_B/G_D（DAG最终纳入代理）；both默认四曲线、区块大小仅eventual六曲线；区块大小仅MB",
    )
    parser.add_argument("--strict", action="store_true", help="任一声明点缺失即失败")
    parser.add_argument(
        "--trace-result", type=Path, help="另画一个已有完整DAG运行的状态轨迹"
    )
    # 直接终端部署：这些量只声明绘图预期，
    # main.py的实际参数仍在每个仿真命令中显式设置，并由read_point逐点核对。
    matrix = parser.add_argument_group("直接声明实验矩阵（无需初始化文件）")
    for name in ("rounds", "miners", "q", "difficulty"):
        matrix.add_argument("--" + name, type=int, help="固定基准值；有CLI矩阵时必填")
    matrix.add_argument("--blocksize", type=float, help="固定基准MB；有CLI矩阵时必填")
    for case in CASES:
        matrix.add_argument(
            "--" + case + "-axis",
            nargs="+",
            type=axis_number if case == "blocksize" else int,
            help="该case的预期横轴列表",
        )
    matrix.add_argument(
        "--networks", nargs="+", choices=NETWORKS, help="miners预期网络，默认五种"
    )
    matrix.add_argument("--seeds", nargs="+", type=int, help="预期种子列表，默认7")
    matrix.add_argument(
        "--losses",
        nargs="+",
        choices=("low", "medium", "high"),
        help="预期损失档位，默认三档",
    )
    args = parser.parse_args(argv)
    try:
        matrix_keys = (
            "rounds",
            "miners",
            "q",
            "difficulty",
            "blocksize",
            "networks",
            "seeds",
            "losses",
            *(case + "_axis" for case in CASES),
        )
        direct = any(getattr(args, key) is not None for key in matrix_keys)
        if direct:
            if args.grid:
                raise ValueError("cannot combine --grid and CLI matrix parameters")
            grid = command_grid(vars(args))
            grid_bytes = json.dumps(grid, sort_keys=True, allow_nan=False).encode(
                "utf-8"
            )
            grid_source = "cli"
        else:
            grid_path = args.grid or args.results_root / "analysis_grid.json"
            grid = load_grid(grid_path)
            grid_bytes = grid_path.read_bytes()
            grid_source = str(grid_path)
        cases = (
            list(CASES)
            if args.case == "all"
            else ["miners"] if args.case == "dag" else [args.case]
        )
        points = selected_points(grid, args.case)
        rows = []
        missing = []
        for point in points:
            folder = args.results_root / point["path"]
            marker = (
                "dag_metrics.json"
                if point["protocol"] == "DAG"
                else "evaluation results.txt"
            )
            if not (folder / marker).exists():
                missing.append(point["path"])
                continue
            try:
                rows.append(read_point(folder, point))
            except (ValueError, KeyError, OSError, TypeError, ZeroDivisionError) as e:
                raise ValueError(str(folder) + ": " + str(e)) from e
        if not rows:
            raise ValueError("no real results found for the selected cases")
        if args.strict and missing:
            raise ValueError(f"{len(missing)} declared points are missing")
        validate_pairs(rows)
        out = args.output_dir or args.results_root / "figures"
        out.mkdir(parents=True, exist_ok=True)
        files = (
            []
            if args.case == "dag"
            else plot_comparisons(rows, grid, out, cases, args.theory, args.throughput)
        )
        if args.case in ("all", "dag"):
            files += plot_dag_features(rows, grid, out)
        if args.trace_result:
            files += plot_trace(args.trace_result, out)
        export_rows(rows, out)
        audit = dict(
            schema_version="chainxim_honest_plot_audit_v3",
            grid=grid,
            throughput=args.throughput,
            case_throughput={
                case: comparison_mode(case, args.throughput) for case in cases
            },
            ledger_scope={
                "Bitcoin": "original num_of_valid_blocks / mining rounds",
                "DAG": "all-node order longest common prefix at mining end / mining rounds; metrics summary or legacy snapshot, excluding genesis; current agreement, not finality",
            },
            throughput_scopes={
                "total": "both protocols: all generated non-genesis blocks / actual mining rounds",
                "eventual": "potential utilization proxy: Bitcoin original main count / rounds; DAG generated / rounds assuming valid blocks eventually disseminate and enter the order; not measured network capacity",
                "current_order": "Bitcoin original main count / rounds; DAG all-node order common prefix / rounds",
                "bitcoin_original_main": "num_of_valid_blocks unchanged: common prefix of global selected chain and height-selected honest nodes, controlled by consensus_miner_quantile; default -1 selects one longest honest node",
                "reference_ordered": "DAG reference-node metric retained for local diagnostics; not used for the common-order curve",
            },
            grid_source=grid_source,
            grid_sha256=hashlib.sha256(grid_bytes).hexdigest(),
            observed_points=len(rows),
            missing_points=missing,
            files=files,
            seed_band="observed min/max, not confidence interval; deterministic PoW seeds need not be independent",
            inputs=[
                dict(
                    path=r["path"],
                    sha256=r["_input_sha256"],
                    network=r["_network"],
                    seed_verified=r["seed_verified"],
                    target_verified=r["target_verified"],
                    target_source=r["target_source"],
                )
                for r in rows
            ],
        )
        (out / "plot_audit.json").write_text(
            json.dumps(audit, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
        )
        if missing:
            print(
                f"Warning: {len(missing)} missing points; no interpolation/zero filling",
                file=sys.stderr,
            )
        print(
            json.dumps(
                dict(
                    observed_points=len(rows),
                    missing_points=len(missing),
                    figures=len(files) // 2,
                    output_dir=str(out),
                ),
                ensure_ascii=False,
            )
        )
        return 0
    except (ValueError, KeyError, OSError, TypeError) as e:
        parser.error(str(e))


if __name__ == "__main__":
    raise SystemExit(main())
