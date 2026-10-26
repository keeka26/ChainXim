"""本地epoch前缀的经验深度确认与可撤销历史。

仅消费只读协议view，不创建图副本，不计算论文风险概率或确定性终局。
"""

from data.dag_events import ConfirmationDelta, ConfluxView, ProtocolDelta


def validate_confirmation(policy, N):
    """原N用于pivot区块深度，保留合法0，拒绝bool/负数/非整数。"""
    if policy not in ("none", "pivot_depth"):
        raise ValueError("confirmation_policy must be none or pivot_depth")
    if type(N) is not int or N < 0:
        raise ValueError("N must be a nonnegative integer")


def compute_confirmed_order(view, policy, N):
    """pivot位置j满足tail_index-j>=N，返回截至epoch_j的全序前缀。

    API可含创世；计数与历史排除创世。none与深度不足返回空tuple。
    rank、引用边数量和仿真时钟不能充当pivot深度。
    """
    validate_confirmation(policy, N)
    cutoff = len(view.pivot) - 1 - N
    if policy == "none" or cutoff < 0:
        return ()
    # DAG增量优化：规范冻结view的order已是epoch拼接，用C层tuple切片复用前缀。
    if type(view) is ConfluxView:
        return view.order[: sum(map(len, view.epochs[: cutoff + 1]))]
    return tuple(h for epoch in view.epochs[: cutoff + 1] for h in epoch)


class ConfirmationTracker:
    """一个共识实例拥有当前prefix、历史首次记录与变更事件；不冻结旧确认。"""

    def __init__(self, policy, N):
        validate_confirmation(policy, N)
        self.policy, self.N = policy, N
        self.current_order = ()  # 当前资格，重组时可能缩短/重排/换成员。
        self.first_confirmed = {}  # 普通块hash -> 首次{round,phase,sequence}，不覆盖。
        self.events = []  # 实际变化事件；重复观察不重复历史计数。
        self.sequence = 0  # 本实例协议边界编号，不是全网事件时钟。
        self.last_delta = ConfirmationDelta.between((), ())
        # DAG性能优化：仅缓存冻结view的相同确认投影，不缓存可变的外部替身。
        self._last_view = None
        self._last_settings = None
        self._steady_delta = None

    def update(self, view, round, phase, *, protocol_delta=None):
        """每接收批末/自产后重算一次；不记录批内临时状态。"""
        self.sequence += 1
        validate_confirmation(self.policy, self.N)
        settings = (self.policy, self.N)
        if (
            type(view) is ConfluxView
            and view is self._last_view
            and settings == self._last_settings
        ):
            if self._steady_delta is None:
                self._steady_delta = ConfirmationDelta._from_shared_prefix(
                    self.current_order, self.current_order, len(self.current_order)
                )
            self.last_delta = self._steady_delta
            return self.last_delta
        new_order = compute_confirmed_order(view, self.policy, self.N)
        # 两个确认序都是各自全序的前缀；相同政策下，可截断已证明的全序共同前缀。
        # 不匹配的外部view/差量及政策切换继续走完整检查路径。
        if (
            type(view) is ConfluxView
            and type(self._last_view) is ConfluxView
            and self._last_settings == settings
            and type(protocol_delta) is ProtocolDelta
            and protocol_delta.old_order is self._last_view.order
            and protocol_delta.new_order is view.order
        ):
            prefix = min(
                protocol_delta.common_prefix_length,
                len(self.current_order),
                len(new_order),
            )
            delta = ConfirmationDelta._from_shared_prefix(
                self.current_order, new_order, prefix
            )
        else:
            delta = ConfirmationDelta.between(self.current_order, new_order)
        genesis = view.pivot[0]
        gained = tuple(h for h in delta.newly_confirmed if h != genesis)
        revoked = tuple(h for h in delta.revoked if h != genesis)
        reconfirmed = tuple(h for h in gained if h in self.first_confirmed)
        point = dict(round=round, phase=phase, sequence=self.sequence)
        for h in gained:
            self.first_confirmed.setdefault(h, dict(point))
        removed = tuple(h for h in delta.removed if h != genesis)
        added = tuple(h for h in delta.added if h != genesis)
        if removed or added:
            self.events.append(
                dict(
                    point,
                    common_prefix_length=delta.common_prefix_length,
                    removed=[h.hex() for h in removed],
                    added=[h.hex() for h in added],
                    revoked=[h.hex() for h in revoked],
                    newly_confirmed=[h.hex() for h in gained],
                    reconfirmed=[h.hex() for h in reconfirmed],
                )
            )
        self.current_order, self.last_delta = new_order, delta
        self._last_view, self._last_settings, self._steady_delta = view, settings, None
        return delta

    def snapshot(self):
        """JSON边界转hex；历史与当前分开，none显式标注disabled。"""
        return dict(
            policy=self.policy,
            N=self.N,
            enabled=self.policy != "none",
            current_order=[h.hex() for h in self.current_order],
            sequence=self.sequence,
            first_confirmed={h.hex(): dict(v) for h, v in self.first_confirmed.items()},
            events=list(self.events),
        )
