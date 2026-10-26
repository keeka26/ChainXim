"""原指标及诚实传播cohort、拒绝和全图分析，全部只读。

原始生成、发布、wire、accepted 与协议事件均取实际处理记录；不读取别人的图供共识使用。
"""

import math

from .dag_analysis import common_order_length


def _delay_summary(values, unfinished, excluded=0):
    """没有完成样本时均值为 None；未完成项不能作为零延迟。"""
    return {
        "completed": len(values),
        "unfinished": unfinished,
        "excluded": excluded,
        "mean_rounds": sum(values) / len(values) if values else None,
    }


def collect_metrics(env):
    """参考诚实节点最小 ID；吞吐分母实际挖矿轮数，排空变化另列。

    当前唯一 order 数不是累加 delta.added；旧 stale/fork/有效块判据无 DAG 对应时 null。
    原网络阈值算法作为 legacy_wire_threshold 观察量保留，不能称为 cohort 百分位。
    """
    reference = env.miners[env.reference_miner_id]
    chain = reference.get_local_chain()
    rounds = env.total_round
    import global_var

    size = global_var.get_blocksize()
    nodes = []
    for miner in env.miners:
        local = miner.get_local_chain()
        nodes.append(
            {
                "miner_id": miner.miner_id,
                "accepted": len(local.block_set) - 1,
                "pending": len(miner.consensus.dependency_resolver.pending),
                "tips": len(local.get_tips()),
                "pivot_height": local.get_height(),
                "max_dag_rank": max((b.dag_rank for b in local.block_set.values())),
                "ordered": len(local.protocol_view.order) - 1,
                "confirmed": None,
                "receive_tape": len(miner.consensus.receive_tape),
                "deferred_forward": len(miner.consensus._deferred_forward),
                "forward_buffer": sum(
                    (len(x) for x in miner._NIC._forward_buffer.values())
                ),
                "output_queue_messages": sum(
                    (len(q) for q in getattr(miner._NIC, "_output_queues", {}).values())
                ),
                "partial_blocks": (
                    len(miner._NIC._dag_sync.parts)
                    if getattr(miner._NIC, "_dag_sync", None)
                    else 0
                ),
                "order_changes": len(miner.dag_protocol_events),
                "pivot_changes": sum(
                    (event["pivot_changed"] for event in miner.dag_protocol_events)
                ),
                "order_removed_blocks": sum(
                    (len(event["removed"]) for event in miner.dag_protocol_events)
                ),
            }
        )
    wire, accepted, dependency = ([], [], [])
    missing_wire = missing_accepted = unresolved_dependency = 0
    excluded = 0
    for blockhash, published in env.first_published_rounds.items():
        source = env.global_chain.block_set[blockhash].blockhead.miner
        for miner in env.miners:
            if miner.miner_id == source or miner.miner_id not in env.honest_miner_ids:
                excluded += 1
                continue
            wr = miner.dag_wire_rounds.get(blockhash)
            ar = miner.dag_accepted_rounds.get(blockhash)
            if wr is None:
                missing_wire += 1
            else:
                wire.append(wr - published)
            if ar is None:
                missing_accepted += 1
            else:
                accepted.append(ar - published)
            if wr is not None:
                if ar is None:
                    unresolved_dependency += 1
                else:
                    dependency.append(ar - wr)
    raw = getattr(
        env.network,
        "ave_block_propagation_times",
        getattr(
            env.network, "stat_prop_times", getattr(env.network, "_stat_prop_times", {})
        ),
    )
    counts = getattr(
        env.network, "block_num_bpt", getattr(env.network, "_block_num_bpt", [])
    )
    original_bpt = {
        rate: value / counts[i] if counts[i] else None
        for (i, (rate, value)) in enumerate(raw.items())
    }
    ordered = len(chain.protocol_view.order) - 1
    mining_ordered = env.ordered_at_mining_end
    # 新增：compact也保存精确LD；只有配置了排空时才另外计算观察末态。
    mining_common = env.common_order_at_mining_end
    common_order = (
        common_order_length(m.get_local_chain().protocol_view.order for m in env.miners)
        if env.dag_settings["drain_rounds"]
        else mining_common
    )
    generated = len(env.generated_rounds)
    pivot = chain.get_height()
    throughput = mining_ordered / rounds if rounds else None
    graph_nodes_equal = all(
        (set(m.get_local_chain().block_set) == set(chain.block_set) for m in env.miners)
    )
    views_equal = all(
        (m.get_local_chain().protocol_view == chain.protocol_view for m in env.miners)
    )
    tape = getattr(
        env.network, "network_tape", getattr(env.network, "_active_links", [])
    )
    queued = sum((n["output_queue_messages"] for n in nodes))
    unfinished = (
        len(tape) + queued
        if env.network.withTopology
        else sum(
            (
                len(getattr(packet, "received_miners", ())) < env.miner_num
                for packet in tape
            )
        )
    )
    stats = {
        "schema_version": "chainxim_conflux_metrics_r11_v1",
        "reference_miner_id": env.reference_miner_id,
        "total_round": rounds,
        "drain_rounds_executed": env.drain_rounds_executed,
        "stop_reason": env.stop_reason,
        "generated": generated,
        "first_published": len(env.first_published_rounds),
        "accepted": len(chain.block_set) - 1,
        "pending": len(reference.consensus.dependency_resolver.pending),
        "invalid": reference.consensus.audit_invalid_inputs,
        "ordered": ordered,
        "confirmed": None,
        "ordered_at_mining_end": mining_ordered,
        "common_order": common_order,
        "common_order_at_mining_end": mining_common,
        "block_throughput_common_order": mining_common / rounds if rounds else None,
        "block_throughput_common_order_after_drain": (
            common_order / rounds if rounds else None
        ),
        "pivot_height": pivot,
        "max_dag_rank": max((b.dag_rank for b in chain.block_set.values())),
        "tips": len(chain.get_tips()),
        "reference_count": sum(
            (len(b.blockhead.refhashes) for b in chain.block_set.values())
        ),
        "block_throughput_ordered": throughput,
        "block_throughput_confirmed": None,
        "block_throughput_ordered_after_drain": ordered / rounds if rounds else None,
        "network_queue_packets": len(tape),
        "queued_transfer_messages": queued,
        "unfinished_network_packets": unfinished,
        "wire_propagation": _delay_summary(wire, missing_wire, excluded),
        "accepted_propagation": _delay_summary(accepted, missing_accepted, excluded),
        "dependency_delay": _delay_summary(dependency, unresolved_dependency),
        "block_propagation_times": original_bpt,
        "local_graphs_equal": graph_nodes_equal,
        "local_views_equal": views_equal,
        "nodes": nodes,
        "effective_hash_attempt_budgets": [
            math.ceil(m.consensus.q) for m in env.miners
        ],
        "num_of_generated_blocks": generated,
        "height_of_longest_chain": max(
            (b.height for b in env.global_chain.block_set.values())
        ),
        "num_of_valid_blocks": None,
        "num_of_stale_blocks": None,
        "stale_rate": None,
        "num_of_forks": None,
        "num_of_heights_with_fork": None,
        "fork_rate": None,
        "average_block_time_main": rounds / pivot if pivot and rounds else None,
        "average_block_time_total": (
            rounds / generated if generated and rounds else None
        ),
        "block_throughput_main": pivot / rounds if rounds else None,
        "block_throughput_total": generated / rounds if rounds else None,
        "throughput_main_MB": pivot * size / rounds if rounds else None,
        "throughput_total_MB": generated * size / rounds if rounds else None,
        "average_chain_growth_in_honest_miners'_chain": sum(
            (n["pivot_height"] for n in nodes if n["miner_id"] in env.honest_miner_ids)
        )
        / len(env.honest_miner_ids),
        "inv_count": sum((m._NIC.inv_count for m in env.miners)),
        "sync_full_chain_count": sum(
            (m._NIC.sync_full_chain_count for m in env.miners)
        ),
        "send_data_count": sum((m._NIC.send_data_count for m in env.miners)),
    }
    stats["not_applicable_reasons"] = {
        "confirmed/block_throughput_confirmed": "confirmation_policy=none; ordering is provisional",
        "invalid": None,
        "num_of_valid_blocks/num_of_stale_blocks/stale_rate/forks/fork_rate": "single-chain exclusion is not a Conflux DAG validity criterion",
        "throughput_zero_rounds": (
            "no mining-round denominator" if rounds == 0 else None
        ),
    }
    stats["metric_scope"] = {
        "generated/first_published": "hash-unique, non-genesis, observer events",
        "accepted/pending/ordered/confirmed/graph": "reference honest node at observation end",
        "block_throughput_ordered": "reference ordered at mining end / actual mining rounds",
        "block_throughput_ordered_after_drain": "reference ordered at observation end / actual mining rounds",
        "common_order": "all-node provisional order longest common prefix at observation end, excluding genesis; not finality",
        "common_order_at_mining_end": "all-node provisional order longest common prefix frozen at actual mining end, excluding genesis",
        "block_throughput_common_order": "common_order_at_mining_end / actual mining rounds",
        "block_throughput_common_order_after_drain": "common_order at observation end / actual mining rounds; drain adds no mining time",
        "main/growth": "reference pivot projection at observation end; not DAG ordered throughput",
        "height_of_longest_chain": "maximum main-parent height in generated observer graph",
        "block_throughput_total": "generated observer blocks / mining rounds",
        "wire/accepted_propagation": "published block x non-producing honest recipient pairs; completed mean + unfinished counts",
        "block_propagation_times": "original network threshold wire algorithm; not cohort percentiles",
        "dependency_delay": "locally accepted round minus first wire-complete round",
        "network_queue_packets/unfinished_network_packets": (
            "in-flight links / in-flight links plus queued per-peer wire messages"
            if env.network.withTopology
            else "physical broadcast packets / packets with missing recipients"
        ),
        "block_propagation_times_topology": "hash-unique complete wire thresholds, includes producer, starts at first actual data send; not cohort percentiles",
        "units": {
            "counts": "blocks (non-genesis)",
            "tips": "blocks (genesis included only in empty graph)",
            "rounds": "simulation rounds",
            "throughput": "blocks/mining-round",
            "size": "nominal MB",
        },
    }
    active = env.dag_dataitems_active
    stats.update(
        dataitems_active=active,
        ordered_dataitems=None,
        confirmed_dataitems=None,
        ordered_dataitems_at_mining_end=None,
        dataitem_throughput_ordered=None,
        dataitem_throughput_confirmed=None,
        dataitem_throughput_ordered_after_drain=None,
    )
    for row, miner in zip(nodes, env.miners):
        row["dataitems"] = (
            miner.dag_dataitems.summary() if miner.dag_dataitems is not None else None
        )
    if active:
        from external import read_ordered_dataitems

        items = read_ordered_dataitems(chain)
        counts = reference.dag_dataitems.summary()
        valid = len(set(items) & env.dataitem_validator)
        occurrences = counts["ordered_occurrences"]
        ordered_blocks = len(chain.protocol_view.order) - 1
        stats.update(
            packed_dataitem_occurrences=counts["packed_occurrences"],
            packed_dataitems=counts["packed_unique"],
            unordered_dataitems=counts["unordered_unique"],
            ordered_dataitem_occurrences=occurrences,
            duplicate_ordered_dataitems=occurrences - len(items),
            ordered_dataitems=len(items),
            ordered_dataitems_at_mining_end=env.ordered_dataitems_at_mining_end,
            dataitem_throughput_ordered=(
                env.ordered_dataitems_at_mining_end / rounds if rounds else None
            ),
            dataitem_throughput_ordered_after_drain=(
                len(items) / rounds if rounds else None
            ),
            valid_dataitem_throughput=(
                env.valid_dataitems_at_mining_end / rounds if rounds else None
            ),
            input_dataitem_rate=(
                len(env.dataitem_validator) / rounds if rounds else None
            ),
            average_dataitems_per_ordered_block=(
                occurrences / ordered_blocks if ordered_blocks else 0
            ),
            block_average_size=(
                occurrences * env.dataitem_params["dataitem_size"] / ordered_blocks
                if ordered_blocks
                else 0
            ),
            valid_dataitem_rate=valid / len(items) if items else 0,
        )
        pivot_body = sum(
            (chain.block_set[h].size for h in chain.protocol_view.pivot[1:])
        )
        generated_body = sum(
            (b.size for b in env.global_chain.block_set.values() if not b.isGenesis)
        )
        stats["throughput_main_MB"] = pivot_body / rounds if rounds else None
        stats["throughput_total_MB"] = generated_body / rounds if rounds else None
    stats["not_applicable_reasons"]["dataitems"] = (
        None if active else "dataitem_enable=False or max_block_capacity=0"
    )
    stats["metric_scope"][
        "dataitems"
    ] = "local accepted packed occurrences/unique; unique current ordered and policy-confirmed prefix"
    stats["metric_scope"][
        "dataitem_throughput_ordered"
    ] = "unique reference ordered items at mining end / actual mining rounds"
    stats["metric_scope"][
        "valid_dataitem_throughput"
    ] = "unique reference mining-end ordered items present in actual environment input set / mining rounds; provisional, not confirmed"
    stats["metric_scope"][
        "block_average_size"
    ] = "nominal body MB per reference current ordered non-genesis block; includes duplicated items, excludes header overhead"
    stats["metric_scope"]["units"]["dataitem_throughput"] = "items/mining-round"
    tracker = reference.consensus.confirmation_tracker
    enabled = tracker.policy != "none"
    stats.update(_confirmation_summary(reference, env.generated_rounds))
    stats.update(
        confirmation_policy=tracker.policy,
        confirmation_N=tracker.N,
        confirmed_at_mining_end=env.confirmed_at_mining_end,
        block_throughput_confirmed=(
            env.confirmed_at_mining_end / rounds if enabled and rounds else None
        ),
        block_throughput_confirmed_after_drain=(
            stats["confirmed"] / rounds if enabled and rounds else None
        ),
    )
    for row, miner in zip(nodes, env.miners):
        row.update(_confirmation_summary(miner, env.generated_rounds))
    if enabled and active:
        current = len(reference.dag_dataitems.confirmed or ())
        stats.update(
            confirmed_dataitems=current,
            confirmed_dataitems_at_mining_end=env.confirmed_dataitems_at_mining_end,
            historically_confirmed_dataitems=len(
                reference.dag_dataitem_first_confirmed
            ),
            dataitem_confirmation_revocation_events=sum(
                (bool(e["revoked"]) for e in reference.dag_dataitem_confirmation_events)
            ),
            dataitem_confirmation_revoked_items=sum(
                (len(e["revoked"]) for e in reference.dag_dataitem_confirmation_events)
            ),
            dataitem_throughput_confirmed=(
                env.confirmed_dataitems_at_mining_end / rounds if rounds else None
            ),
            dataitem_throughput_confirmed_after_drain=(
                current / rounds if rounds else None
            ),
        )
    else:
        stats.update(
            confirmed_dataitems_at_mining_end=None,
            historically_confirmed_dataitems=None,
            dataitem_confirmation_revocation_events=None,
            dataitem_confirmation_revoked_items=None,
            dataitem_throughput_confirmed_after_drain=None,
        )
    stats["local_confirmations_equal"] = (
        all(
            (
                m.consensus.get_confirmed_order() == tracker.current_order
                for m in env.miners
            )
        )
        if enabled
        else None
    )
    stats["not_applicable_reasons"]["confirmed/block_throughput_confirmed"] = (
        None if enabled else "confirmation_policy=none; ordering is provisional"
    )
    stats["metric_scope"].update(
        confirmation="reversible empirical local pivot_depth policy, not deterministic finality or paper risk probability",
        historically_confirmed_blocks="unique non-genesis blocks ever confirmed at this reference node, includes subsequently revoked",
        confirmation_delay="actual generation to reference first confirmation; completed mean, unfinished never-confirmed accepted blocks, excluded missing generation records",
        confirmation_revoked_blocks="block membership loss occurrences across local boundaries; repeated losses counted; not unique blocks",
        confirmation_rollback_blocks="confirmation sequence suffix rollback occurrences, including later replayed same members",
        confirmation_revocation_events="local boundaries with at least one block membership loss",
        block_throughput_confirmed="reference current confirmed blocks at mining end / actual mining rounds",
        block_throughput_confirmed_after_drain="reference current confirmed blocks at observation end / actual mining rounds",
        dataitem_throughput_confirmed="reference unique current confirmed items at mining end / actual mining rounds",
        dataitem_throughput_confirmed_after_drain="reference unique current confirmed items at observation end / actual mining rounds",
    )
    stats["metric_scope"]["units"].update(
        confirmation_revocation_events="local protocol boundaries",
        confirmation_revoked_blocks="block membership loss occurrences (non-genesis)",
        confirmation_delay="simulation rounds",
        dataitem_confirmation_revoked_items="unique-item membership loss occurrences per boundary",
    )
    from .dag_analysis import propagation_cohorts, pending_summary, graph_summary

    stats["propagation_cohorts"] = propagation_cohorts(env)
    stats["pending_dependencies"] = pending_summary(
        reference, env.total_round + env.drain_rounds_executed
    )
    stats.update(graph_summary(chain))
    for row, m in zip(nodes, env.miners):
        row["invalid"] = m.consensus.audit_invalid_inputs
        row["pending_dependencies"] = pending_summary(
            m, env.total_round + env.drain_rounds_executed
        )
        row.update(graph_summary(m.get_local_chain()))
    honest = nodes
    stats["honest_node_aggregates"] = {
        key: dict(
            min=min((row[key] for row in honest)),
            max=max((row[key] for row in honest)),
            mean=sum((row[key] for row in honest)) / len(honest),
        )
        for key in ("accepted", "pending", "ordered", "tips", "invalid")
    }
    stats["tips_time_series"] = [
        dict(round=e["round"], phase=e["phase"], tips=e["tips"])
        for e in reference.dag_state_history
    ]
    stats["metric_scope"].update(
        invalid="complete static block rejection input occurrences; not unique claimed IDs or partial-fragment drops",
        propagation_cohorts="per-block honest non-producing recipients frozen at first actual publication; ceil thresholds; empty cohort excluded",
        pending_dependencies="current resolver candidates; age since first pending protocol boundary; no unfinished-as-zero delay",
        epoch_sizes="current provisional local epoch membership, excluding genesis epoch",
    )
    return stats


def _confirmation_summary(miner, generated_rounds):
    """本节点当前/历史/撤销次数与首次确认延迟。

    延迟用实际生成记录而非可声明的timestamp；撤销blocks单位为成员退出次数，
    同块反复退出可计多次。序后缀回滚另列，不能把重排交集当实际失去确认。
    """
    tracker = miner.consensus.confirmation_tracker
    enabled = tracker.policy != "none"
    empty = dict(
        confirmed=None,
        historically_confirmed_blocks=None,
        confirmation_changes=None,
        confirmation_revocation_events=None,
        confirmation_revoked_blocks=None,
        confirmation_rollback_blocks=None,
        confirmation_delay=None,
    )
    if not enabled:
        return empty
    ordinary = {
        h for (h, b) in miner.get_local_chain().block_set.items() if not b.isGenesis
    }
    delays, unfinished, excluded = ([], 0, 0)
    for h in ordinary:
        generated = generated_rounds.get(h)
        if generated is None:
            excluded += 1
        elif h not in tracker.first_confirmed:
            unfinished += 1
        else:
            delays.append(tracker.first_confirmed[h]["round"] - generated)
    return dict(
        confirmed=max(0, len(tracker.current_order) - 1),
        historically_confirmed_blocks=len(tracker.first_confirmed),
        confirmation_changes=len(tracker.events),
        confirmation_revocation_events=sum(
            (bool(e["revoked"]) for e in tracker.events)
        ),
        confirmation_revoked_blocks=sum((len(e["revoked"]) for e in tracker.events)),
        confirmation_rollback_blocks=sum((len(e["removed"]) for e in tracker.events)),
        confirmation_delay=_delay_summary(delays, unfinished, excluded),
    )
