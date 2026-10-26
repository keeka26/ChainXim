"""完整块输入及协议边界的只读记录。

记录的是实际提交给resolver的本地批次；声明差量供重放核对，不能替代重新验证。
compact不保留大事件，但拒绝计数、pending年龄和工程计时仍有效。
"""

import math

from data import Block
from data.dag_events import ProtocolDelta

EVENT_SCHEMA = "chainxim_conflux_input_event_r11_v1"


def _scalar(value):
    """保留JSON标量类型；不可表示的非法字段标记，不执行repr/自定义转换。"""
    if value is None or type(value) in (bool, int, str):
        return value
    if type(value) is float and math.isfinite(value):
        return value
    return {"invalid_type": type(value).__name__}


def _hex(value):
    """仅bytes转hex，非法类型不伪装成合法协议字节。"""
    return (
        value.hex() if type(value) is bytes else {"invalid_type": type(value).__name__}
    )


def block_payload(block):
    """复制头/正文/声明hash及本地元数据；不携parentblock/next远端邻接。

    非Block及不可编码字段仍可作为拒绝输入记录；decoder把类型标记还原为None再拒绝。
    """
    if not isinstance(block, Block):
        return {"unsupported_input_type": type(block).__name__}
    h = getattr(block, "blockhead", None)
    refs = getattr(h, "refhashes", None)
    return dict(
        name=_scalar(getattr(block, "name", None)),
        blockhash=_hex(getattr(block, "blockhash", None)),
        header_version=_scalar(getattr(h, "header_version", None)),
        miner=_scalar(getattr(h, "miner", None)),
        timestamp=_scalar(getattr(h, "timestamp", None)),
        content=_hex(getattr(h, "content", None)),
        target=_hex(getattr(h, "target", None)),
        nonce=_scalar(getattr(h, "nonce", None)),
        prehash=_hex(getattr(h, "prehash", None)),
        refhashes=(
            [_hex(x) for x in refs]
            if type(refs) is tuple
            else {"invalid_type": type(refs).__name__}
        ),
        height=_scalar(getattr(block, "height", None)),
        dag_rank=_scalar(getattr(block, "dag_rank", None)),
        isGenesis=_scalar(getattr(block, "isGenesis", None)),
        isAdversaryBlock=_scalar(getattr(block, "isAdversaryBlock", None)),
        size_MB=_scalar(getattr(block, "size", None)),
        segment_num=_scalar(getattr(block, "segment_num", None)),
    )


def delta_payload(delta):
    """序后缀差量，重复边界不输出完整order。确认成员差量与重排分开。"""
    result = dict(
        common_prefix_length=delta.common_prefix_length,
        removed=[h.hex() for h in delta.removed],
        added=[h.hex() for h in delta.added],
    )
    if hasattr(delta, "revoked"):
        result.update(
            revoked=[h.hex() for h in delta.revoked],
            newly_confirmed=[h.hex() for h in delta.newly_confirmed],
        )
    return result


def record_boundary(c, round, phase):
    """协议安装后记录实际批次/差量；pending年龄在compact也维护。"""
    pending = set(c.dependency_resolver.pending)
    c.audit_pending_first = {h: c.audit_pending_first.get(h, round) for h in pending}
    if not c.audit_enabled:
        return
    chain = c.local_chain
    # DAG增量优化：完整审计仍逐边界记录，accepted差量从真实接纳日志取得。
    accepted_log = chain._dag_accepted_hashes
    same_log = getattr(c, "_audit_accepted_log", None) is accepted_log
    added = (
        accepted_log[c._audit_accepted_count :]
        if same_log
        else [h for h in accepted_log if h not in c._audit_accepted]
    )
    pivot = chain.protocol_view.pivot
    if pivot is c._audit_pivot:
        pivot_delta = dict(common_prefix_length=len(pivot), removed=[], added=[])
    else:
        cache = getattr(chain, "_dag_view_cache", None)
        if (
            cache is not None
            and cache.view is chain.protocol_view
            and cache.previous_view is not None
            and c._audit_pivot is cache.previous_view.pivot
        ):
            delta = ProtocolDelta._from_shared_prefix(
                c._audit_pivot, pivot, cache.common_pivot_length
            )
        else:
            delta = ProtocolDelta.between(c._audit_pivot, pivot)
        pivot_delta = delta_payload(delta)
    c.audit_events.append(
        dict(
            schema_version=EVENT_SCHEMA,
            kind="protocol_boundary",
            miner_id=c.miner_id,
            sequence=c.confirmation_tracker.sequence,
            round=round,
            phase=phase,
            origin=c._audit_origin,
            blocks=c._audit_batch,
            rejections=c._audit_rejections,
            dataitem_inputs=c._audit_dataitem_inputs,
            accepted=sorted(h.hex() for h in added),
            pending_added=sorted(h.hex() for h in pending - c._audit_pending),
            pending_removed=sorted(h.hex() for h in c._audit_pending - pending),
            protocol_delta=delta_payload(chain.last_protocol_delta),
            pivot_delta=pivot_delta,
            confirmation_delta=delta_payload(c.confirmation_tracker.last_delta),
            invalid_inputs=c.audit_invalid_inputs,
        )
    )
    if same_log:
        c._audit_accepted.update(added)
    else:
        c._audit_accepted = set(accepted_log)
    c._audit_accepted_log, c._audit_accepted_count = accepted_log, len(accepted_log)
    c._audit_pending, c._audit_pivot = pending, pivot
    c._audit_batch, c._audit_rejections, c._audit_dataitem_inputs = [], [], []
    c._audit_origin = "received"


def annotate_dataitems(miner, phase):
    """保持原Miner观察phase，与共识receive_batch的phase分开。

    记录声明项计数/本边界确认差量；离线读者必须从真实INSERT和块正文重算。
    """
    c = miner.consensus
    if not c.audit_enabled or not c.audit_events:
        return
    event = c.audit_events[-1]
    event["observation_phase"] = phase
    state = miner.dag_dataitems
    if state is None:
        event["dataitem_state"] = None
        event["dataitem_confirmation_delta"] = None
        return
    event["dataitem_state"] = state.summary()
    event["dataitem_confirmation_delta"] = next(
        (
            dict(e)
            for e in reversed(miner.dag_dataitem_confirmation_events)
            if e["sequence"] == event["sequence"]
        ),
        None,
    )


def node_events(miner):
    """版本化开始/边界/结束帧；结束帧检测截断，pending允许有限结束。

    未处理tape与拒绝尾单列，不把未应用输入偷偷接纳；不是网络/PoW运行检查点。
    """
    from .dag_output import graph_snapshot

    c = miner.consensus
    if not c.audit_enabled:
        raise ValueError("compact output has no complete input replay events")
    start = dict(
        schema_version=EVENT_SCHEMA,
        kind="node_start",
        miner_id=miner.miner_id,
        genesis=block_payload(c.local_chain.head),
    )
    end = dict(
        schema_version=EVENT_SCHEMA,
        kind="node_end",
        miner_id=miner.miner_id,
        boundary_count=len(c.audit_events),
        graph=graph_snapshot(c.local_chain),
        pending=sorted(h.hex() for h in c.dependency_resolver.pending),
        confirmation=c.confirmation_tracker.snapshot(),
        invalid_inputs=c.audit_invalid_inputs,
        unprocessed_blocks=[block_payload(b) for b in c.receive_tape],
        unprocessed_rejections=list(c._audit_rejections),
        unapplied_dataitem_inputs=list(c._audit_dataitem_inputs),
        dataitems=(
            miner.dag_dataitems.snapshot() if miner.dag_dataitems is not None else None
        ),
        dataitem_first_confirmed={
            h.hex(): dict(v) for h, v in miner.dag_dataitem_first_confirmed.items()
        },
        dataitem_confirmation_events=list(miner.dag_dataitem_confirmation_events),
    )
    return [start, *c.audit_events, end]


def refresh_generated_wire_metadata(c, block):
    """原网络preprocessing完成后补自产大小/分片，头/正文声明保持原样。

    数据项块size在原Miner中按实际正文计算；重放不再次运行网络预处理。
    """
    if c.audit_enabled and c.audit_events:
        frame = c.audit_events[-1]
        if frame["origin"] == "generated":
            for row in frame["blocks"]:
                if row["blockhash"] == block.blockhash.hex():
                    row["size_MB"], row["segment_num"] = block.size, block.segment_num
