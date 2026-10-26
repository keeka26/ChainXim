"""只读完整DAG/缺父及诚实接收者传播分析。"""

import math


def common_order_length(orders):
    """新增：比较各节点已有全序的同位置hash，返回排除创世的共同前缀长度。

    只持有序列引用，不复制图或生成hex快照；最短序列长度不能替代逐位置比较。
    """
    orders = tuple(orders)
    if not orders:
        raise ValueError("common order requires node sequences")
    prefix = 0
    for hashes in zip(*orders):
        if any(h != hashes[0] for h in hashes[1:]):
            break
        prefix += 1
    return max(0, prefix - 1)


def graph_summary(chain):
    """本地当前投影的边/epoch口径；不累计排序撤销重放。"""
    view = chain.protocol_view
    sizes = [len(e) for e in view.epochs[1:]]
    ordinary = len(chain.block_set) - 1
    return dict(
        main_parent_count=ordinary,
        full_parent_count=ordinary
        + sum(len(b.blockhead.refhashes) for b in chain.block_set.values()),
        unassigned_epoch_blocks=ordinary - (len(view.order) - 1),
        epoch_sizes=sizes,
        max_epoch_size=max(sizes, default=0),
        mean_epoch_size=sum(sizes) / len(sizes) if sizes else None,
    )


def pending_summary(miner, end_round):
    """仍缺父候选的实际等待年龄/缺父身份；有限结束不制造接纳或延迟0。"""
    c = miner.consensus
    blocks = [
        dict(
            blockhash=h.hex(),
            first_pending_round=c.audit_pending_first[h],
            age_rounds=max(0, end_round - c.audit_pending_first[h]),
            missing=sorted(x.hex() for x in c.dependency_resolver.missing[h]),
        )
        for h in sorted(c.dependency_resolver.pending)
    ]
    ages = [b["age_rounds"] for b in blocks]
    return dict(
        blocks=blocks,
        count=len(blocks),
        max_age_rounds=max(ages) if ages else None,
        mean_age_rounds=sum(ages) / len(ages) if ages else None,
    )


def propagation_cohorts(env):
    """首次实际公开的诚实非生产者cohort；ceil阈值，多个阈值独立计算。

    预先知道的接收者记初始覆盖/0，而不是负传播；原pair及网络wire统计另保留。
    """
    rates = (0.5, 0.9, 1.0)
    samples = {kind: {str(r): [] for r in rates} for kind in ("wire", "accepted")}
    unfinished = {kind: {str(r): 0 for r in rates} for kind in samples}
    excluded = 0
    rows = []
    for h, published in sorted(
        env.first_published_rounds.items(), key=lambda x: (x[1], x[0])
    ):
        ids = env.publication_cohorts[h]
        row = dict(
            blockhash=h.hex(), published_round=published, recipient_ids=list(ids)
        )
        if not ids:
            excluded += 1
        for kind, attr in (
            ("wire", "dag_wire_rounds"),
            ("accepted", "dag_accepted_rounds"),
        ):
            arrival = [getattr(env.miners[i], attr).get(h) for i in ids]
            known = sorted(max(0, t - published) for t in arrival if t is not None)
            row[kind + "_prepublished_recipients"] = [
                i for i, t in zip(ids, arrival) if t is not None and t < published
            ]
            row[kind + "_threshold_delays"] = {}
            for r in rates:
                key = str(r)
                needed = math.ceil(r * len(ids))
                delay = known[needed - 1] if needed and len(known) >= needed else None
                row[kind + "_threshold_delays"][key] = delay
                if not ids:
                    continue
                if delay is None:
                    unfinished[kind][key] += 1
                else:
                    samples[kind][key].append(delay)
        rows.append(row)
    result = {
        "blocks": rows,
        "scope": "frozen honest non-producer cohort; already-known initial coverage separately identified",
    }
    for kind in samples:
        result[kind] = {}
        for key, values in samples[kind].items():
            incomplete = unfinished[kind][key]
            denominator = len(values) + incomplete
            result[kind][key] = dict(
                completed=len(values),
                unfinished=incomplete,
                excluded=excluded,
                mean_rounds=sum(values) / len(values) if values else None,
                completion_fraction=len(values) / denominator if denominator else None,
            )
    return result
