"""Conflux 2018 v4 Fig.3–6全量算法与内部增量投影。

输入只有原生Chain的本地accepted闭合图；不读全局记录器、pending、其他节点。
单位树子树权重；引用只进入inclusive Past和epoch；Fig.6整批ready排序。
不会修改图、链尾、回调或已安装协议状态，不提供第二持久store。
"""

from data import Chain
from data.dag_events import ConfluxView


def _check_chain(chain: Chain):
    """要求固定v2创世、完整全边/树边/深度缓存；PoW由接纳层负责。"""
    if not isinstance(chain, Chain) or not chain._dag_mode:
        raise ValueError("Conflux protocol requires a native accepted DAG")
    return chain._inspect_dag()


def _subtree_weights(root: bytes, tree_children: dict) -> dict[bytes, int]:
    """论文Fig.3：迭代树后序，每节点计一次；树子包括全部合法旁支，引用不计。"""
    order = []
    stack = [root]
    while stack:
        h = stack.pop()
        order.append(h)
        stack.extend(tree_children[h])
    weights = {}
    for h in reversed(order):
        weights[h] = 1 + sum(weights[c] for c in tree_children[h])
    return weights


def _batch_order(chain: Chain, epoch: frozenset[bytes]) -> tuple[bytes, ...]:
    """论文Fig.6：仅计epoch内部依赖；整个ready批按hash追加后才释放下一批。

    不能用逐个最小堆：A<C<B且A解锁C时，初始ready A/B必须先输出A,B。
    """
    remaining = {h: sum(p in epoch for p in chain.parents_by_hash[h]) for h in epoch}
    ready = sorted(h for h, n in remaining.items() if n == 0)
    result = []
    while ready:
        result.extend(ready)
        following = []
        for h in ready:
            for c in chain.children_by_hash[h]:
                if c in remaining:
                    remaining[c] -= 1
                    if remaining[c] == 0:
                        following.append(c)
        ready = sorted(following)
    if len(result) != len(epoch):
        raise ValueError("epoch cannot be ordered because dependencies are cyclic")
    return tuple(result)


def _view_for_pivot(chain: Chain, pivot: tuple[bytes, ...]) -> ConfluxView:
    """给定合法主父路径，按inclusive Past差集构造逐epoch有序投影。

    此helper也核查可信快照中的历史已安装view；历史尾可以因新接纳而暂非树叶。
    """
    if (
        not pivot
        or pivot[0] != chain.head.blockhash
        or any(h not in chain.block_set for h in pivot)
        or any(
            chain.block_set[h].blockhead.prehash != p for p, h in zip(pivot, pivot[1:])
        )
    ):
        raise ValueError("pivot must be a local main-parent path from genesis")
    # DAG性能优化：已分配集合是祖先闭合的。遇到其中的块即可停止回溯，
    # 无需为每个pivot块重走同一段历史，也不永久缓存平方规模的Past集合。
    assigned = set()
    epochs = []
    for h in pivot:
        epoch = set()
        stack = [h]
        while stack:
            current = stack.pop()
            if current in assigned or current in epoch:
                continue
            epoch.add(current)
            stack.extend(chain.parents_by_hash[current])
        epochs.append(_batch_order(chain, frozenset(epoch)))
        assigned.update(epoch)
    epochs = tuple(epochs)
    return ConfluxView(pivot, epochs, tuple(h for epoch in epochs for h in epoch))


def _compute_view_from_tree(chain: Chain, tree_children: dict) -> ConfluxView:
    """共享同一套pivot/epoch规则，调用方负责图的信任边界。"""
    root = chain.head.blockhash
    weights = _subtree_weights(root, tree_children)
    pivot = [root]
    while tree_children[pivot[-1]]:
        pivot.append(min(tree_children[pivot[-1]], key=lambda h: (-weights[h], h)))
    return _view_for_pivot(chain, tuple(pivot))


def _compute_accepted_view(chain: Chain) -> ConfluxView:
    """DAG内部热路径：只消费接纳层已维护的本地树，不重验全部历史头。

    add_dag_block已核查每个新头、全父和深度；恢复同样完整验证并重建索引。
    此入口不接受网络对象、外部view或快照，公共compute_view继续完整检查。
    """
    chain._require_dag()
    # DAG增量优化：缓存不参与序列化；恢复后的首次调用才重建全部派生状态。
    cache = getattr(chain, "_dag_view_cache", None)
    if cache is None:
        cache = _AcceptedViewCache(chain)
        chain._dag_view_cache = cache
        return cache.view
    return cache.update(chain)


class _AcceptedViewCache:
    """已验证本地图的增量投影；只存hash/权重，不拥有第二份Block或图。

    主父权重沿新块祖先更新；仅旧pivot上选择改变的位置需要重建后缀。
    已接纳块的Past不会因后续接纳改变，因此相同pivot前缀的epoch可直接复用。
    公共compute_view始终使用上面的独立全量算法，不读取此缓存。
    """

    def __init__(self, chain: Chain):
        tree = {
            h: tuple(child.blockhash for child in block.next)
            for h, block in chain.block_set.items()
        }
        self.weights = _subtree_weights(chain.head.blockhash, tree)
        self.best_child = {
            h: min(children, key=lambda c: (-self.weights[c], c))
            for h, children in tree.items()
            if children
        }
        pivot = [chain.head.blockhash]
        while pivot[-1] in self.best_child:
            pivot.append(self.best_child[pivot[-1]])
        self.view = _view_for_pivot(chain, tuple(pivot))
        self.positions = {h: i for i, h in enumerate(self.view.pivot)}
        self.epoch_by_hash = {}
        self.epoch_ends = []
        count = 0
        for i, epoch in enumerate(self.view.epochs):
            self.epoch_by_hash.update((h, i) for h in epoch)
            count += len(epoch)
            self.epoch_ends.append(count)
        self.seen_count = len(chain._dag_accepted_hashes)
        # 这三个值只给相同旧/新view对象提供可证明的前缀下界。
        self.previous_view = None
        self.common_pivot_length = self.common_order_length = 0

    def update(self, chain: Chain) -> ConfluxView:
        """按真实接纳顺序更新；一个接收批次仍只安装一次协议状态。"""
        old = self.view
        dirty = len(old.pivot)
        for h in chain._dag_accepted_hashes[self.seen_count :]:
            self.weights[h] = 1
            branch = chain.block_set[h]
            while branch.parentblock is not None:
                parent = branch.parentblock
                ph, bh = parent.blockhash, branch.blockhash
                self.weights[ph] += 1
                best = self.best_child.get(ph)
                # 当前最优子树增加权重后仍为最优，不必重新比较同一对象。
                if best is None or (
                    best != bh and (-self.weights[bh], bh) < (-self.weights[best], best)
                ):
                    self.best_child[ph] = bh
                    position = self.positions.get(ph)
                    if position is not None:
                        dirty = min(dirty, position)
                branch = parent
        self.seen_count = len(chain._dag_accepted_hashes)
        self.previous_view = old
        if dirty == len(old.pivot):
            self.common_pivot_length = len(old.pivot)
            self.common_order_length = len(old.order)
            return old

        pivot = list(old.pivot[: dirty + 1])
        while pivot[-1] in self.best_child:
            pivot.append(self.best_child[pivot[-1]])
        pivot = tuple(pivot)
        common = dirty + 1
        while (
            common < min(len(old.pivot), len(pivot))
            and old.pivot[common] == pivot[common]
        ):
            common += 1
        self.common_pivot_length = common
        self.common_order_length = self.epoch_ends[common - 1]
        if common == len(old.pivot) == len(pivot):
            return old

        # DAG增量优化：先撤销旧后缀的epoch成员资格，再用同一批次排序规则重算。
        prefix_size = self.common_order_length
        for h in old.order[prefix_size:]:
            del self.epoch_by_hash[h]
        epochs = list(old.epochs[:common])
        ends = self.epoch_ends[:common]
        suffix = []
        for p in pivot[common:]:
            epoch = set()
            stack = [p]
            while stack:
                h = stack.pop()
                if h in self.epoch_by_hash or h in epoch:
                    continue
                epoch.add(h)
                stack.extend(chain.parents_by_hash[h])
            ordered = _batch_order(chain, frozenset(epoch))
            self.epoch_by_hash.update((h, len(epochs)) for h in ordered)
            epochs.append(ordered)
            suffix.extend(ordered)
            ends.append(prefix_size + len(suffix))
        # 前缀来自同一冻结view；epoch映射证明新增集合互斥，排序沿用原算法。
        view = ConfluxView._from_validated(
            pivot, tuple(epochs), old.order[:prefix_size] + tuple(suffix)
        )
        for h in old.pivot[common:]:
            del self.positions[h]
        self.positions.update((h, i) for i, h in enumerate(pivot[common:], common))
        self.epoch_ends, self.view = ends, view
        return view


def compute_view(chain: Chain) -> ConfluxView:
    """论文Fig.4/6的严格公共入口：完整检查后选择最重子树，再算epochs/order。

    引用边、height、dag_rank、全图past大小均不作为pivot权重。
    order仅覆盖pivot尾的Past；其他accepted块仍保存在原Chain中。
    """
    _, _, tree_children, _ = _check_chain(chain)
    return _compute_view_from_tree(chain, tree_children)


def validate_view(chain: Chain, view: ConfluxView, *, current: bool = True) -> None:
    """安装必须等于当前规范view；可信恢复只核查历史主父/past/epoch/order。

    current=False不表示已验收最重选择，只允许批处理接纳与安装之间的旧投影。
    """
    if not isinstance(view, ConfluxView):
        raise ValueError("expected an immutable ConfluxView")
    if current:
        expected = compute_view(chain)
    else:
        _check_chain(chain)
        expected = _view_for_pivot(chain, view.pivot)
    if view != expected:
        raise ValueError(
            "protocol view disagrees with canonical pivot epochs or batch order"
        )
