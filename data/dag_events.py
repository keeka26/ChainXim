"""只读协议投影与撤销/重放差量。

内部仅使用32字节hash tuple，不拥有Block、图副本或确认状态。
确认差量复用前缀记录；完整输入另由dag_audit持久记录。排序并不等于确认。
"""

from dataclasses import dataclass


def _hash_tuple(value, *, empty=True):
    """冻结记录只接受规范tuple、32字节bytes及唯一元素。"""
    if (
        type(value) is not tuple
        or (not empty and not value)
        or any(type(h) is not bytes or len(h) != 32 for h in value)
        or len(set(value)) != len(value)
    ):
        raise ValueError("expected a tuple of unique 32-byte hashes")


@dataclass(frozen=True, slots=True)
class ConfluxView:
    """pivot含创世；epochs为逐epoch有序tuple；order是epochs拼接。

    形状在构造时校验，实际主父/past/批次排序及当前最重选择由协议层核查。
    """

    pivot: tuple[bytes, ...]
    epochs: tuple[tuple[bytes, ...], ...]
    order: tuple[bytes, ...]

    def __post_init__(self):
        """拒绝可变容器、重复hash和epoch/pivot/order形状不一致。"""
        _hash_tuple(self.pivot, empty=False)
        _hash_tuple(self.order, empty=False)
        if type(self.epochs) is not tuple or len(self.epochs) != len(self.pivot):
            raise ValueError("one epoch is required per pivot block")
        for p, epoch in zip(self.pivot, self.epochs):
            _hash_tuple(epoch, empty=False)
            if p not in epoch:
                raise ValueError("each pivot block must belong to its epoch")
        if self.epochs[0] != (self.pivot[0],) or self.order != tuple(
            h for e in self.epochs for h in e
        ):
            raise ValueError("genesis epoch and concatenated order disagree")

    @classmethod
    def _from_validated(cls, pivot, epochs, order):
        """DAG增量内部入口：组装已证明的冻结前缀和规范新epoch。

        仅供accepted-view缓存调用：保留部分来自原冻结view；新增成员来自
        已验证图的无重复epoch集合及_batch_order。公共构造器仍完整核查。
        避免每次追加一个epoch都重新校验全部历史hash和epoch。
        """
        if not pivot or len(pivot) != len(epochs) or not order:
            raise ValueError("invalid trusted projection shape")
        view = object.__new__(cls)
        for name, value in (("pivot", pivot), ("epochs", epochs), ("order", order)):
            object.__setattr__(view, name, value)
        return view


@dataclass(frozen=True, slots=True)
class ProtocolDelta:
    """最长共同前缀之后，先撤销removed再应用added；两者允许重排交集。"""

    old_order: tuple[bytes, ...]
    new_order: tuple[bytes, ...]
    common_prefix_length: int
    removed: tuple[bytes, ...]
    added: tuple[bytes, ...]

    def __post_init__(self):
        """长度必须是实际最长前缀，后缀不得漏项或混入前缀。"""
        for values in (self.old_order, self.new_order, self.removed, self.added):
            _hash_tuple(values)
        n = self.common_prefix_length
        if type(n) is not int or not 0 <= n <= min(
            len(self.old_order), len(self.new_order)
        ):
            raise ValueError("invalid common prefix length")
        if (
            self.old_order[:n] != self.new_order[:n]
            or n < min(len(self.old_order), len(self.new_order))
            and self.old_order[n] == self.new_order[n]
            or self.removed != self.old_order[n:]
            or self.added != self.new_order[n:]
        ):
            raise ValueError("delta must contain the exact longest-prefix suffixes")

    @classmethod
    def between(cls, old_order: tuple[bytes, ...], new_order: tuple[bytes, ...]):
        """按hash比较前缀；重复安装返回空removed/added，不累计唯一吞吐。"""
        _hash_tuple(old_order)
        _hash_tuple(new_order)
        n = 0
        while n < min(len(old_order), len(new_order)) and old_order[n] == new_order[n]:
            n += 1
        return cls(old_order, new_order, n, old_order[n:], new_order[n:])

    @classmethod
    def _from_shared_prefix(cls, old_order, new_order, prefix):
        """DAG内部差量：调用者已用同一冻结view证明prefix之前全部相同。

        序列唯一性由规范view保证；只向后寻找实际最长共同前缀，再截取后缀。
        不作为外部输入入口，公共between及构造器仍执行全部序列/形状检查。
        """
        if type(prefix) is not int or not 0 <= prefix <= min(
            len(old_order), len(new_order)
        ):
            raise ValueError("invalid trusted prefix length")
        n = prefix
        while n < min(len(old_order), len(new_order)) and old_order[n] == new_order[n]:
            n += 1
        delta = object.__new__(cls)
        for name, value in (
            ("old_order", old_order),
            ("new_order", new_order),
            ("common_prefix_length", n),
            ("removed", old_order[n:]),
            ("added", new_order[n:]),
        ):
            object.__setattr__(delta, name, value)
        return delta


@dataclass(frozen=True, slots=True)
class ConfirmationDelta(ProtocolDelta):
    """序后缀回滚与确认成员实际退出分别表示。

    removed/added用于撤销和重放序列，可有交集；revoked仅表示失去确认成员资格。
    """

    @property
    def revoked(self):
        """旧确认序中不再属于新确认序的hash，保留旧序顺序。"""
        # DAG增量优化：唯一序列的共同前缀不可能再次出现在任一后缀。
        new = set(self.added)
        return tuple(h for h in self.removed if h not in new)

    @property
    def newly_confirmed(self):
        """本次获得确认资格的hash，可能是历史上确认过的项。"""
        old = set(self.removed)
        return tuple(h for h in self.added if h not in old)
