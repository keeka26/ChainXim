"""原Miner拥有的数据项状态，不另建仿真器/图存储。

本地accepted表示已打包，ProtocolDelta决定临时ordered，confirmed来自本地经验确认前缀。
普通抽象项按8字节身份去重；没有余额、签名、双花冲突或真实交易执行。
"""

from collections import Counter

from data.dag_events import ProtocolDelta
from external import split_dataitems


class ConfluxDataitemState:
    """保留已知项与有序出现次数，避免重复项随一次撤销就错误回池。"""

    def __init__(self, queue_enabled=True):
        """全知输入模式关闭本地池；打包/排序状态仍独立维护。"""
        self.queue_enabled = queue_enabled
        self._known = {}  # bytes -> None，首次观察顺序，重组时不丢项。
        self.input_items = {}  # 本节点实际INSERT过的唯一项，不是全局队列。
        self.packed = {}  # accepted blockhash -> tuple[8-byte item]，含旁支。
        self._ordered_counts = Counter()  # item -> 当前order中的出现次数，可撤销。
        self._applied_order = ()  # 已消费的协议序；重复观察不重复应用delta。
        self.confirmed = None  # None=关闭；tuple=当前唯一确认项，可撤销。

    def ingest(self, content):
        """消费真实INSERT，重复输入幂等；不因已有块打包就认定完成。"""
        for item in split_dataitems(content):
            self.input_items.setdefault(item, None)
            self._known.setdefault(item, None)

    @property
    def pool(self):
        """待打包项按首次观察顺序返回；只排除当前ordered，不按自产删除。"""
        return (
            tuple(x for x in self._known if not self._ordered_counts[x])
            if self.queue_enabled
            else ()
        )

    @property
    def ordered(self):
        """当前order内按首次出现位置的唯一项，不是累计delta.added项。"""
        return tuple(
            dict.fromkeys(
                x for h in self._applied_order for x in self.packed.get(h, ())
            )
        )

    def select(self, content, capacity):
        """本地模式从池取前capacity项；全知模式从环境输入列表取项。"""
        self.ingest(content)
        items = (
            self.pool
            if self.queue_enabled
            else tuple(dict.fromkeys(split_dataitems(content)))
        )
        return b"".join(items[:capacity])

    def synchronize(self, chain):
        """登记本地accepted，再先逆序撤销removed、正序重放added。

        仅首次观察允许bootstrap；后续要求delta.old_order与已消费序吻合，防止漏掉边界。
        相同序仅更新已打包旁支，不重复计算有序出现次数。数据项从不查询全局生成图。
        """
        for h, block in chain.block_set.items():
            if h not in self.packed:
                items = (
                    () if block.isGenesis else split_dataitems(block.blockhead.content)
                )
                self.packed[h] = items
                for item in items:
                    self._known.setdefault(item, None)
        new_order = chain.protocol_view.order
        if new_order == self._applied_order:
            return
        delta = chain.last_protocol_delta
        if not self._applied_order:
            delta = ProtocolDelta.between((), new_order)
        if (
            delta is None
            or delta.old_order != self._applied_order
            or delta.new_order != new_order
        ):
            raise ValueError("dataitem state missed a ProtocolDelta boundary")
        for h in reversed(delta.removed):
            for item in reversed(self.packed[h]):
                self._ordered_counts[item] -= 1
                if self._ordered_counts[item] == 0:
                    del self._ordered_counts[item]
        for h in delta.added:
            self._ordered_counts.update(self.packed[h])
        self._applied_order = new_order

    def set_confirmed_order(self, order):
        """单独同步确认；order未变也可能有确认变化。

        只接受已应用全序的前缀；按8-byte身份去重。pool仍按ordered排除。
        """
        if order is None:
            self.confirmed = None
            return
        if type(order) is not tuple or self._applied_order[: len(order)] != order:
            raise ValueError("confirmed order must be a prefix of applied order")
        self.confirmed = tuple(
            dict.fromkeys(x for h in order for x in self.packed.get(h, ()))
        )

    def summary(self):
        """只读计数；accepted中同项多次出现和当前ordered唯一数分别统计。"""
        occurrences = sum(len(items) for items in self.packed.values())
        packed = {x for items in self.packed.values() for x in items}
        return {
            "input_unique": len(self.input_items),
            "packed_occurrences": occurrences,
            "packed_unique": len(packed),
            "ordered_occurrences": sum(self._ordered_counts.values()),
            "ordered_unique": len(self._ordered_counts),
            "confirmed_unique": (
                len(self.confirmed) if self.confirmed is not None else None
            ),
            "unordered_unique": len(packed - set(self._ordered_counts)),
            "pool_unique": len(self.pool),
        }

    def snapshot(self):
        """按hex保存数据项身份；状态快照不替代完整输入重放。"""
        return {
            "queue_enabled": self.queue_enabled,
            "counts": self.summary(),
            "pool": [x.hex() for x in self.pool],
            "ordered": [x.hex() for x in self.ordered],
            "confirmed": (
                [x.hex() for x in self.confirmed]
                if self.confirmed is not None
                else None
            ),
            "packed": {
                h.hex(): [x.hex() for x in items]
                for h, items in self.packed.items()
                if items
            },
        }
