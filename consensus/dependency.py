"""原生Chain的多父依赖resolver。

仅持有未接纳消息和等待索引，accepted块仍只在Chain.block_set中。
不挖矿、不选pivot；注入的validate只核对静态头/PoW，不执行图接纳。
"""

import copy
import heapq
from typing import Callable

from data import Block, Chain
from .conflux_codec import validate_block_header


class DependencyResolver:
    """全父闭合后迭代激活，返回本次首次接纳的本地Block元组。

    pending/missing/waiting均由本对象维护；不要直接修改其中的头或集合。
    无效声明不加入永久拒绝集合，不能阻止后来同ID的真实合法消息。
    """

    def __init__(self, chain: Chain, validate: Callable[[Block], bool]):
        """允许空Chain等待创世；不允许将非空旧单链当DAG使用。"""
        if not isinstance(chain, Chain):
            raise TypeError("chain must be a native Chain")
        if not callable(validate):
            raise TypeError("validate must be callable")
        if chain.block_set and not chain._dag_mode:
            raise ValueError("DependencyResolver requires an empty or v2 DAG Chain")
        self.chain = chain
        self.validate = validate
        self.pending: dict[bytes, Block] = {}
        self.missing: dict[bytes, set[bytes]] = {}
        self.waiting: dict[bytes, set[bytes]] = {}
        self._ready: list[bytes] = []
        self._scheduled: set[bytes] = set()

    def _valid(self, block: Block) -> bool:
        """总是先验证声明身份，不能被宽松回调绕过；回调异常显式传播。"""
        return (
            isinstance(block, Block)
            and validate_block_header(block)
            and bool(self.validate(block))
        )

    def _schedule(self, blockhash: bytes):
        """确定性ready堆，同一候选最多排队一次；不是协议全序。"""
        if blockhash not in self._scheduled:
            heapq.heappush(self._ready, blockhash)
            self._scheduled.add(blockhash)

    def _unlink_waiting(self, blockhash: bytes):
        """移除候选对旧缺父的等待关系，清理空键，避免残留激活。"""
        for parent in self.missing.get(blockhash, ()):
            dependents = self.waiting.get(parent)
            if dependents is not None:
                dependents.discard(blockhash)
                if not dependents:
                    del self.waiting[parent]

    def _refresh(self, blockhash: bytes):
        """仅accepted能满足依赖；重算也处理通过Chain外部显式插入的父。"""
        self._unlink_waiting(blockhash)
        block = self.pending[blockhash]
        parents = (
            ()
            if block.isGenesis
            else (block.blockhead.prehash, *block.blockhead.refhashes)
        )
        missing = {parent for parent in parents if parent not in self.chain.block_set}
        self.missing[blockhash] = missing
        for parent in missing:
            self.waiting.setdefault(parent, set()).add(blockhash)
        if not missing:
            self._schedule(blockhash)

    def _forget(self, blockhash: bytes):
        """接纳或无效候选离开pending；不建立拒绝ID缓存。"""
        self._unlink_waiting(blockhash)
        self.missing.pop(blockhash, None)
        self.pending.pop(blockhash, None)

    def _release(self, parent_hash: bytes):
        """父有效接纳后刷新所有等待者；全父到齐才进入ready。"""
        for child in sorted(tuple(self.waiting.get(parent_hash, ()))):
            if child in self.pending:
                self._refresh(child)

    def submit(self, block: Block) -> tuple[Block, ...]:
        """验证/隔离输入并迭代级联，只返回本次首次接纳的本地块。

        普通wire块的height/rank不是身份承诺，副本中重置None，接纳时按本地全父计算。
        无效输入返回空元组；校验器异常传播。merge观察回调异常发生于存储提交后，
        仍清理pending并保留ready；修复回调后再次submit可继续，无重复接纳/通知。
        """
        if not self._valid(block):
            return ()
        blockhash = block.blockhash
        if blockhash in self.chain.block_set:
            # 外部显式接纳后再通知：先清除该块旧pending/缺父索引，再激活等待子块。
            self._forget(blockhash)
            self._release(blockhash)
        elif blockhash not in self.pending:
            owned = copy.deepcopy(block)
            if not owned.isGenesis:
                owned.height = owned.dag_rank = None
            self.pending[blockhash] = owned
            self._refresh(blockhash)
        else:
            # 新的重复输入不能覆盖已验证的自有头；身份已在查重前核对。
            self._refresh(blockhash)
        activated = []
        while self._ready:
            current = heapq.heappop(self._ready)
            self._scheduled.discard(current)
            if current not in self.pending:
                continue
            candidate = self.pending[current]
            try:
                valid = self._valid(candidate)
            except Exception:
                self._schedule(current)
                raise
            if not valid:
                self._forget(current)
                continue
            # 进入原子接纳前离开pending，merge观察回调也能看到互斥状态。
            self._forget(current)
            try:
                local, added = self.chain.add_dag_block(candidate, deepcopy=False)
            except Exception as error:
                if current in self.chain.block_set:
                    # 回调失败时存储已提交，必须维持accepted/pending互斥和正确等待索引。
                    self._forget(current)
                    self._release(current)
                    raise
                if isinstance(error, ValueError):
                    self._forget(current)
                    continue
                self.pending[current] = candidate
                self._refresh(current)
                raise
            if added:
                activated.append(local)
            self._release(current)
        return tuple(activated)
