import copy
import logging
import heapq  # 恢复时确定性拓扑遍历
from collections import defaultdict

import graphviz
import matplotlib.pyplot as plt

import global_var

from .block import Block
from .dag_events import ConfluxView, ProtocolDelta  # 只读投影
from functions import INT_LEN, BYTE_ORDER

logger = logging.getLogger(__name__)


# 图验证只计算临时索引，不在检查失败时改写本地图。
def _dag_structure(blocks, head_hash, last_hash):
    """由已承诺的头重建全边/树边和深度，拒绝缺父、混合根及循环。

    用于可信本地DAG的复制/恢复，不是PoW验证或网络反序列化。
    返回的索引为新对象；所有检查通过后调用方才安装邻接。
    """
    from consensus.conflux_codec import validate_block_header
    if not isinstance(blocks, dict) or not blocks:
        raise ValueError('DAG snapshot must contain the fixed genesis')
    if head_hash not in blocks or last_hash not in blocks:
        raise ValueError('DAG head and selected tail must be accepted hashes')
    parents, children, tree_children = {}, {h:set() for h in blocks}, {h:[] for h in blocks}
    roots=[]
    for h,block in blocks.items():
        if not isinstance(block,Block) or h != block.blockhash or not validate_block_header(block):
            raise ValueError('DAG block key/header/claimed hash is invalid')
        if block.isGenesis:
            roots.append(h)
            dependencies=()
        else:
            dependencies=(block.blockhead.prehash,*block.blockhead.refhashes)
        if any(parent not in blocks for parent in dependencies):
            raise ValueError('DAG snapshot has missing dependencies')
        parents[h]=dependencies
        for parent in dependencies:
            children[parent].add(h)
        if dependencies:
            tree_children[dependencies[0]].append(h)
    if roots != [head_hash]:
        raise ValueError('DAG must have exactly its fixed genesis as head')
    remaining={h:len(deps) for h,deps in parents.items()}
    ready=[h for h,count in remaining.items() if count==0]
    heapq.heapify(ready)
    heights, ranks = {}, {}
    while ready:
        h=heapq.heappop(ready)
        deps=parents[h]
        height=heights[deps[0]]+1 if deps else 0
        rank=max(ranks[parent] for parent in deps)+1 if deps else 0
        block=blocks[h]
        if type(block.height) is not int or block.height!=height or type(block.dag_rank) is not int or block.dag_rank!=rank:
            raise ValueError('DAG snapshot height/rank disagrees with dependencies')
        heights[h],ranks[h]=height,rank
        for child in children[h]:
            remaining[child]-=1
            if remaining[child]==0:
                heapq.heappush(ready,child)
    if len(heights)!=len(blocks):
        raise ValueError('DAG snapshot contains a dependency cycle')
    for values in tree_children.values():
        values.sort()
    tips={h for h,values in children.items() if not values}
    return parents,children,tree_children,tips

class Chain(object):

    def __init__(self, miner_id = None):
        self.miner_id = miner_id
        self.head = None
        self.last_block = self.head  # 指向最新区块，代表矿工认定的主链
        self.block_set = {}
        self.switch_tracker_callback = None # 用于记录链尾切换事件
        # self.merge_tracker_callback = None # 用于记录区块并入事件
        self.merge_callback = None # 用于在Environment处理区块并入事件
        self._init_dag_indexes()  # 默认仍为旧单链
        '''
        默认的共识机制中会对chain添加一个创世区块
        默认情况下chain不可能为空
        '''

    def __getstate__(self):
        # DAG快照只存块本身和hash角色，索引恢复时重建。
        if self._dag_mode:
            self._inspect_dag()
            self._check_protocol_projection()  # 检查已安装历史投影
            return {'miner_id':self.miner_id,'_dag_mode':True,'_dag_snapshot_version':1,
                    'head':self.head.blockhash,'last_block':self.last_block.blockhash,
                    'protocol_view':self.protocol_view,  # delta通知不重放
                    'block_set':{h:copy.deepcopy(block) for h,block in self.block_set.items()},
                    'switch_tracker_callback':None,'merge_callback':None}
        chain = copy.deepcopy(self)
        for block in chain.block_set.values():
            block.parentblock = None
            block.next = [block.blockhash for block in block.next]
        chain.switch_tracker_callback = None
        # chain.merge_tracker_callback = None
        chain.last_block = self.last_block.blockhash
        return vars(chain)

    def __setstate__(self, state):
        # 完整检查临时Chain后安装，失败不替换已有对象。
        if state.get('_dag_mode',False) is True:
            if type(state.get('_dag_snapshot_version')) is not int or state['_dag_snapshot_version']!=1:
                raise ValueError('unsupported DAG snapshot version')
            temporary=Chain(state.get('miner_id'))
            temporary._install_dag(state['block_set'],state['head'],state['last_block'])
            temporary._restore_protocol_view(state.get('protocol_view'))  # 兼容无协议view的旧快照
            self.__dict__.clear()
            self.__dict__.update(temporary.__dict__)
            return
        # v2快照不得伪装旧格式绕过全边恢复。
        if state.get('_dag_mode',False) is not False or any(getattr(block.blockhead,'header_version',1)!=1 for block in state['block_set'].values()):
            raise ValueError('DAG blocks cannot use legacy snapshot restore')
        self.__dict__.update(state)
        self._init_dag_indexes()  # 旧快照缺新增字段时补默认值
        for block in self.block_set.values():
            if block.height != 0:
                block.parentblock = self.block_set[block.blockhead.prehash]
            block.next = [self.block_set[hash] for hash in block.next]
        self.last_block = self.block_set[self.last_block]

    def __deepcopy__(self, memo):
        # 第一遍每hash复制一次，第二遍连接本地树邻接。
        if self._dag_mode:
            self._inspect_dag()
            copy_chain=Chain(self.miner_id)
            memo[id(self)]=copy_chain
            blocks={h:copy.deepcopy(block,memo) for h,block in self.block_set.items()}
            copy_chain._install_dag(blocks,self.head.blockhash,self.last_block.blockhash)
            self._check_protocol_projection()  # 
            copy_chain._restore_protocol_view(self.protocol_view)  # 
            return copy_chain
        if self.head is None:
            return None
        copy_chain = Chain(miner_id=self.miner_id)
        copy_chain.head = copy.deepcopy(self.head)
        copy_chain.block_set[copy_chain.head.blockhash] = copy_chain.head
        memo[id(copy_chain.head)] = copy_chain.head
        q = [copy_chain.head]
        q_o = [self.head]
        copy_chain.last_block = copy_chain.head
        while q_o:
            for block in q_o[0].next:
                copy_block = copy.deepcopy(block, memo)
                copy_block.parentblock = q[0]
                q[0].next.append(copy_block)
                q.append(copy_block)
                q_o.append(block)
                memo[id(copy_block)] = copy_block
                copy_chain.block_set[copy_block.blockhash] = copy_block
                if block.name == self.last_block.name:
                    copy_chain.last_block = copy_block
            q.pop(0)
            q_o.pop(0)
        return copy_chain
    
    # 原生Chain的DAG存储区；不选pivot、不执行PoW。
    def _init_dag_indexes(self):
        """默认旧模式；全边索引与tips仅由DAG接纳维护。"""
        self._dag_mode=False
        self.parents_by_hash={}
        self.children_by_hash={}
        self._dag_tips=set()
        # 最近协议边界的只读缓存；存储接纳不自动选尾。
        self.protocol_view:ConfluxView|None=None
        self.last_protocol_delta:ProtocolDelta|None=None

    def _require_dag(self):
        """空Chain可查询空tips，非空旧链不能冒充有全边索引的DAG。"""
        if self.block_set and not self._dag_mode:
            raise ValueError('DAG queries require a native v2 DAG chain')

    def _inspect_dag(self):
        """验证头、闭合和本地缓存/邻接一致，拒绝损坏图被复制或导出。"""
        if self.head is None or self.last_block is None:
            raise ValueError('DAG head and selected tail are required')
        indexes=_dag_structure(self.block_set,self.head.blockhash,self.last_block.blockhash)
        parents,children,tree_children,tips=indexes
        if self.head is not self.block_set[self.head.blockhash] or self.last_block is not self.block_set[self.last_block.blockhash]:
            raise ValueError('DAG head/tail must refer to canonical local objects')
        if self.parents_by_hash!=parents or self.children_by_hash!=children or self._dag_tips!=tips:
            raise ValueError('DAG full-edge indexes disagree with block headers')
        for h,block in self.block_set.items():
            parent=self.block_set[parents[h][0]] if parents[h] else None
            if block.parentblock is not parent:
                raise ValueError('DAG local main parent is inconsistent')
            expected=[self.block_set[child] for child in tree_children[h]]
            if len(block.next)!=len(expected) or any(a is not b for a,b in zip(block.next,expected)):
                raise ValueError('DAG local tree children contain extra/missing/cyclic links')
        return indexes

    def _install_dag(self, blocks, head_hash, last_hash):
        """全部验证成功后重建本地树对象连接，引用边始终仅在hash索引中。"""
        parents,children,tree_children,tips=_dag_structure(blocks,head_hash,last_hash)
        for h,block in blocks.items():
            block.parentblock=blocks[parents[h][0]] if parents[h] else None
            block.next=[blocks[child] for child in tree_children[h]]
        self.block_set=blocks
        self.head=blocks[head_hash]
        self.last_block=blocks[last_hash]
        self.parents_by_hash=parents
        self.children_by_hash=children
        self._dag_tips=tips
        self._dag_mode=True
        # DAG性能优化：恢复/复制后的图必须重新计算当前规范view，历史view不作缓存。
        self._dag_canonical_view=None
        self._dag_steady_delta=None
        # DAG增量优化：恢复只重建接纳日志/rank，协议权重及投影缓存延迟重建。
        self._dag_view_cache=None
        self._dag_accepted_hashes=list(blocks)
        self._dag_max_rank=max(block.dag_rank for block in blocks.values())

    # 协议投影安装区，不替代唯一block_set。
    def _check_protocol_projection(self):
        """可信复制/导出核查最近view，允许接纳批处理中暂未重算的历史选择。"""
        if self.protocol_view is not None:
            from consensus.conflux_protocol import validate_view
            validate_view(self,self.protocol_view,current=False)
            if self.last_block.blockhash!=self.protocol_view.pivot[-1]:
                raise ValueError('installed view and selected tail disagree')

    def _restore_protocol_view(self,view:ConfluxView|None):
        """恢复派生历史view，不重复发出撤销/应用通知；无协议view的旧快照默认为None。"""
        if view is None:
            return
        from consensus.conflux_protocol import validate_view
        validate_view(self,view,current=False)
        if self.last_block.blockhash!=view.pivot[-1]:
            raise ValueError('snapshot view and selected tail disagree')
        self.protocol_view=view
        self.last_protocol_delta=ProtocolDelta.between(view.order,view.order)

    def set_protocol_view(self,view:ConfluxView)->ProtocolDelta:
        """先核对当前规范view，再原子安装尾/order/LCP delta。

        旧后缀必须先撤销，新后缀再重放；added可能包含过去已排序的块。
        仅尾hash真正改变时调用原switch回调；回调异常在安装提交后传播。
        """
        from consensus.conflux_protocol import validate_view
        validate_view(self,view)
        # 公共安装入口仍完整验证；缓存仅在DAG对象上创建，不进入旧单链状态。
        self._dag_canonical_view=view
        # 公共安装已独立验证；避免把更早的增量缓存误用为其前缀证明。
        self._dag_view_cache=None
        return self._apply_protocol_view(view)

    def _refresh_protocol_view(self)->ProtocolDelta:
        """DAG性能优化：同一accepted图复用不可变规范view，仍安装本次空差量。

        合法拓扑变更只能经add_dag_block或_install_dag，两者都会使缓存失效。
        新块及恢复由接纳层完整验证；已接纳块的协议头/邻接不得由调用者原地改写。
        公共安装、复制/恢复和最终校验各自保留完整检查，不依赖此运行时缓存。
        """
        view=getattr(self,'_dag_canonical_view',None)
        if view is None:
            from consensus.conflux_protocol import _compute_accepted_view
            view=_compute_accepted_view(self)
            self._dag_canonical_view=view
        return self._apply_protocol_view(view)

    def _apply_protocol_view(self,view:ConfluxView)->ProtocolDelta:
        """只由已校验/计算的内部路径调用；不重复计算刚刚得到的规范结果。"""
        old_order=self.protocol_view.order if self.protocol_view is not None else ()
        # 不变的view是冻结对象；空差量可复用，避免逐轮反复校验整条相同序列。
        if view is self.protocol_view:
            delta=getattr(self,'_dag_steady_delta',None)
            if delta is None:
                # 相同冻结view已证明整序相同，首次空差量也无需扫描历史。
                delta=ProtocolDelta._from_shared_prefix(old_order,view.order,len(old_order))
                self._dag_steady_delta=delta
        else:
            # DAG增量优化：只有缓存的旧/新view对象均吻合，才接受共享前缀证明。
            cache=getattr(self,'_dag_view_cache',None)
            if cache is not None and cache.previous_view is self.protocol_view and cache.view is view:
                delta=ProtocolDelta._from_shared_prefix(old_order,view.order,cache.common_order_length)
            else:
                delta=ProtocolDelta.between(old_order,view.order)
            self._dag_steady_delta=None
        tail=self.block_set[view.pivot[-1]]
        changed=self.last_block is not tail
        self.protocol_view,self.last_protocol_delta,self.last_block=view,delta,tail
        if changed and self.switch_tracker_callback:
            self.switch_tracker_callback(tail)
        return delta

    def get_total_order(self)->tuple[Block,...]:
        """返回最近安装order的原生本地对象；未安装返回空，不自动计算。"""
        self._require_dag()
        return tuple(self.block_set[h] for h in self.protocol_view.order) if self.protocol_view else ()

    def get_parent_hashes(self, blockhash: bytes) -> tuple[bytes,...]:
        """返回主父在前、规范引用在后的全依赖；创世为空，未知hash抛KeyError。"""
        self._require_dag()
        return self.parents_by_hash[blockhash]

    def get_main_children(self, blockhash: bytes) -> tuple[Block,...]:
        """仅树子，按hash排序返回本地对象；引用子不混入next。"""
        self._require_dag()
        block=self.block_set[blockhash]
        return tuple(sorted(block.next,key=lambda value:value.blockhash))

    def get_tips(self) -> tuple[Block,...]:
        """无任何已接纳子（树边/引用边均计）的块；pending不在Chain。"""
        self._require_dag()
        return tuple(self.block_set[h] for h in sorted(self._dag_tips))

    def get_past(self, blockhash: bytes, include_self: bool = True) -> frozenset[bytes]:
        """迭代沿全父边查询祖先闭包；visited使钻石共享祖先只访问一次。"""
        self._require_dag()
        if blockhash not in self.block_set:
            raise KeyError(blockhash)
        visited=set()
        stack=[blockhash]
        while stack:
            h=stack.pop()
            if h in visited:
                continue
            visited.add(h)
            stack.extend(self.parents_by_hash[h])
        if not include_self:
            visited.remove(blockhash)
        return frozenset(visited)

    def add_dag_block(self, block: Block, *, deepcopy: bool = True) -> tuple[Block,bool]:
        """原子/幂等接纳v2块；先验证身份和全父，再计算/核查height/rank。

        不执行PoW或诚实选父规则：resolver/共识先做静态与PoW回调。
        缺父或非法输入抛ValueError，失败不改图或调用者。deepcopy=False转移对象所有权。
        成功merge_callback看到完整索引；回调异常在存储提交后向调用者传播。
        """
        from consensus.conflux_codec import validate_block_header
        if not isinstance(block,Block) or not validate_block_header(block):
            raise ValueError('invalid Conflux v2 block identity')
        if self.block_set and not self._dag_mode:
            raise ValueError('legacy and Conflux blocks cannot share a Chain')
        dependencies=() if block.isGenesis else (block.blockhead.prehash,*block.blockhead.refhashes)
        if any(parent not in self.block_set for parent in dependencies):
            raise ValueError('all valid DAG parents must be accepted before insertion')
        height=self.block_set[dependencies[0]].height+1 if dependencies else 0
        rank=max(self.block_set[parent].dag_rank for parent in dependencies)+1 if dependencies else 0
        for label,want in [('height',height),('dag_rank',rank)]:
            value=getattr(block,label)
            if value is not None and (type(value) is not int or value!=want):
                raise ValueError(f'{label} disagrees with accepted dependencies')
        if block.blockhash in self.block_set:
            return self.block_set[block.blockhash],False
        if block.isGenesis and self.head is not None:
            raise ValueError('a DAG cannot contain another genesis')
        local=copy.deepcopy(block) if deepcopy else block
        local.height,local.dag_rank=height,rank
        local.parentblock=self.block_set[dependencies[0]] if dependencies else None
        local.next=[]
        self.block_set[local.blockhash]=local
        self.parents_by_hash[local.blockhash]=dependencies
        self.children_by_hash[local.blockhash]=set()
        self._dag_tips.add(local.blockhash)
        for parent in dependencies:
            self.children_by_hash[parent].add(local.blockhash)
            self._dag_tips.discard(parent)
        if dependencies:
            local.parentblock.next.append(local)
            local.parentblock.next.sort(key=lambda value:value.blockhash)
        else:
            self.head=self.last_block=local
        self._dag_mode=True
        # DAG增量优化：只在DAG对象上创建；日志供协议和统计消费，唯一存储仍是block_set。
        if not hasattr(self,'_dag_accepted_hashes'):
            self._dag_accepted_hashes=[]
            self._dag_max_rank=0
        self._dag_accepted_hashes.append(local.blockhash)
        self._dag_max_rank=max(self._dag_max_rank,local.dag_rank)
        # 必须先失效再通知merge回调，回调可能立即读取当前图或继续接纳。
        self._dag_canonical_view=None
        self._dag_steady_delta=None
        if self.merge_callback:
            self.merge_callback(local)
        return local,True

    def __is_empty(self):
        if self.head is None:
            # print("Chain Is empty")
            return True
        else:
            return False

    ## chain数据层主要功能区↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓

    def search_block_by_hash(self, blockhash: bytes):
        # 利用区块哈希，搜索某块是否存在(搜索树)
        # 存在返回区块地址，不存在返回None
        return self.block_set.get(blockhash, None)

    def search_block(self, block: Block):
        # 利用区块哈希，搜索某块是否存在(搜索树)
        # 存在返回区块地址，不存在返回None
        if self.head is None or block is None:
            return None
        else:
            return self.block_set.get(block.blockhash)

    def get_last_block(self):  # 返回最深的block，空链返回None
        return self.last_block

    def set_last_block(self,block:Block):
        # DAG只允许显式选择accepted本地对象，插入不自动选尾。
        if self._dag_mode:
            local=self.search_block(block)
            if local is not None:
                # 旁路手动换尾清理已安装缓存，避免投影自相矛盾。
                if self.last_block is not local:
                    self.protocol_view=self.last_protocol_delta=None
                self.last_block=local
                if self.switch_tracker_callback:
                    self.switch_tracker_callback(local)
            return
        # 设置本链最深的块
        # 本块必须是在链中的
        if self.search_block(block):
            self.last_block = block
            if self.switch_tracker_callback:
                self.switch_tracker_callback(block)
        else:
            return

    def get_height(self,block:Block=None):
        # 默认返回最深区块的高度
        # 或者返回指定区块的高度
        if block is not None:
            return block.get_height()
        else:
            return self.get_last_block().get_height()

    def add_blocks(self, blocks: Block | list[Block], insert_point:Block=None,
                   deepcopy: bool = True, auto_switch: bool = True):
        # 旧逆序链批次不承担图接纳；v1主体逐句保留。
        if self._dag_mode:
            raise ValueError('use add_dag_block for a DAG; legacy chain batches are not graph batches')
        if blocks and any(getattr(item.blockhead,'header_version',1)!=1 for item in ((blocks,) if isinstance(blocks,Block) else blocks)):
            raise ValueError('v2 blocks must use add_dag_block')
        if not blocks:
            return
        # 添加区块的功能 (深拷贝*)
        # item是待添加的内容 可以是list[Block]类型 也可以是Block类型
        # 即可以批量添加也可以只添加一个区块
        # 批量添加时blocks按顺序构成哈希链，第一个区块是最新区块
        # inset_point 是插入区块的位置 从其后开始添加 默认为最深链
        '''
        添加区块的功能不会考虑区块前后的hash是否一致
        这个任务不属于数据层 是共识层的任务
        数据层只负责添加
        '''
        add_block_list:list[Block]=[]
        if insert_point is None:
            if isinstance(blocks,Block):
                insert_point = self.search_block_by_hash(blocks.blockhead.prehash)
            else:
                insert_point = self.search_block_by_hash(blocks[-1].blockhead.prehash)
            assert self.__is_empty() or insert_point is not None

        if isinstance(blocks,Block):
            if (cur2add := self.search_block(blocks)) is not None:
                return cur2add
            add_block_list.append(copy.deepcopy(blocks) if deepcopy else blocks)
        else:
            checklist = copy.copy(blocks)
            while checklist:
                cur2add = checklist.pop()
                # 寻找第一个不在链中的区块
                if (local_block := self.search_block(cur2add)) is not None:
                    insert_point = local_block
                else:
                    checklist.append(cur2add)
                    break
            add_block_list.extend(copy.deepcopy(checklist) if deepcopy else checklist)

        # 处理特殊情况
            # 如果当前区块为空 添加blocklist的第一个区块
            # 默认这个特殊情况是不会被触发的
            # 只有consens里的创世区块会触发 其他情况无视
        if self.__is_empty():
            self.head = add_block_list.pop()
            self.block_set[self.head.blockhash] = self.head
            self.set_last_block(self.head)
        cur2add = self.head

        while add_block_list:
            cur2add = add_block_list.pop() # 提取当前待添加区块list中的一个
            cur2add.parentblock = insert_point # 设置它的父节点
            insert_point.next.append(cur2add)  # 设置父节点的子节点
            cur2add.next = []            # 初始化它的子节点
            insert_point = cur2add             # 父节点设置为它
            self.block_set[cur2add.blockhash] = cur2add # 将它加入blockset中

        if self.merge_callback:
            self.merge_callback(cur2add)

        # if self.merge_tracker_callback:
        #     self.merge_tracker_callback(cur2add)

        # 如果新加的区块的高度比现在的链的高度高 重新将lastblock指向新加区块
        if auto_switch and cur2add.get_height() > self.get_height():
            self.set_last_block(cur2add)
        return cur2add

    def add_block_forcibly(self, block: Block):
        # 仅沿主父搬链不能补齐引用依赖，显式拒绝DAG。
        if self._dag_mode or (block is not None and getattr(block.blockhead,'header_version',1)!=1):
            raise ValueError('legacy forced insertion cannot repair a DAG')
        # 该功能是强制将该区块加入某条链 一般不被使用与共识中
        # 返回值：深拷贝插入完之后新插入链的块头
        # block 的 last必须不为none
        # 不会为block默认赋值 要求使用该方法必须给出添加的区块 否则提示报错

        copylist:list[Block] = []  # 需要拷贝过去的区块list
        local_tmp = self.search_block(block)
        while block and not local_tmp:
            copylist.append(block)
            block = block.parentblock
            local_tmp = self.search_block(block)

        newest_block = None
        if len(copylist) > 0 and local_tmp:
            # 返回深拷贝的最后一个区块的指针
            return self.add_blocks(blocks=copylist,insert_point=local_tmp)
        elif len(copylist) == 0 and local_tmp:
            # 如果没拷贝且block已经在链上，返回链上的block
            return local_tmp
        else:
            # 链头不一致
            return None

    def delete_block(self,block:Block=None):
        '''
        没有地方被使用
        就先不管了
        可能的使用场景：
        矿工不需要记录自身的区块链视野 可以只记录主链
        需要删除分叉
        '''
        pass
    ## chain数据层主要功能区↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
        


    ## chain数据层外部方法区↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
        # 主要用于计算 展示链的相关数据
    def ShowBlock(self):  # 按从上到下从左到右展示block,打印块名
        if not self.head:
            print()
        q = [self.head]
        blocklist = []
        while q:
            block = q.pop(0)
            blocklist.append(block)
            print("{};".format(block.name), end="")
            for i in block.next:
                q.append(i)
        print("")
        return blocklist

    def InversShowBlock(self):
        # 返回逆序的主链
        cur = self.get_last_block()
        blocklist = []
        while cur:
            # print(cur.name)
            blocklist.append(cur)
            cur = cur.parentblock
        return blocklist

    def ShowLChain(self):
        # 打印主链
        blocklist = self.InversShowBlock()
        blocklist.reverse()
        for i in blocklist:
            print("{}→→→→".format(i.name), end="")
        print("")
        return blocklist

    def ShowStructure1(self):
        # 打印树状结构
        blocklist = [self.head]
        printnum = 1
        while blocklist:
            length = 0
            print("|    ", end="")
            print("-|   " * (printnum - 1))
            while printnum > 0:
                blocklist.extend(blocklist[0].next)
                blockprint = blocklist.pop(0)
                length += len(blockprint.next)
                print("{}   ".format(blockprint.name), end="")
                printnum -= 1
            print("")
            printnum = length

    def ShowStructure(self, miner_num=10):
        # 打印树状结构
        # 可能需要miner数量 也许放在这里不是非常合适？
        plt.figure()
        plt.rcParams['font.size'] = 16
        plt.rcParams['font.family'] = 'Times New Roman'
        blocktmp = self.head
        fork_list = []
        while blocktmp:
            if blocktmp.isGenesis is False:
                rd2 = blocktmp.blockhead.timestamp + blocktmp.blockhead.miner / miner_num
                rd1 = blocktmp.parentblock.blockhead.timestamp + blocktmp.parentblock.blockhead.miner / miner_num
                ht2 = blocktmp.height
                ht1 = ht2 - 1
                if blocktmp.isAdversaryBlock:
                    plt.scatter(rd2, ht2, color='r', marker='o')
                    plt.plot([rd1, rd2], [ht1, ht2], color='r')
                else:
                    plt.scatter(rd2, ht2, color='b', marker='o')
                    plt.plot([rd1, rd2], [ht1, ht2], color='b')
            else:
                plt.scatter(0, 0, color='b', marker='o')
            list_tmp = copy.copy(blocktmp.next)
            if list_tmp:
                blocktmp = list_tmp.pop(0)
                fork_list.extend(list_tmp)
            else:
                if fork_list:
                    blocktmp = fork_list.pop(0)
                else:
                    blocktmp = None
        plt.xlabel('Round')
        plt.ylabel('Block Height')
        plt.grid(True)
        RESULT_PATH = global_var.get_result_path()
        plt.savefig(RESULT_PATH / 'blockchain visualisation.svg')
        if global_var.get_show_fig():
            plt.show()
        plt.close()

    def ShowStructureWithGraphviz(self,other_list:list[int] = None):
        '''借助Graphviz将区块链可视化'''
        # 采用有向图
        dot = graphviz.Digraph('Blockchain Structure',engine='dot')
        blocktmp = self.head
        fork_list = []
        while blocktmp:
            if blocktmp.isGenesis is False:
                # 建立区块节点
                if blocktmp.isAdversaryBlock:
                    dot.node(blocktmp.name, shape='rect', color='red')
                else:
                    if other_list is None:
                        dot.node(blocktmp.name,shape='rect',color='blue')
                    else:
                        dot.node(blocktmp.name,shape='rect',color='orange' if blocktmp.blockhead.miner in other_list else 'blue')
                # 建立区块连接
                dot.edge(blocktmp.parentblock.name, blocktmp.name)
            else:
                dot.node('B0',shape='rect',color='black',fontsize='20')
            list_tmp = copy.copy(blocktmp.next)
            if list_tmp:
                blocktmp = list_tmp.pop(0)
                fork_list.extend(list_tmp)
            else:
                if fork_list:
                    blocktmp = fork_list.pop(0)
                else:
                    blocktmp = None
        # 生成矢量图,展示结果
        dot.render(directory=global_var.get_result_path() / "blockchain_visualization",
                   format='svg', view=global_var.get_show_fig())

    def GetBlockIntervalDistribution(self):
        stat = []
        blocktmp2 = self.last_block
        height = blocktmp2.height
        while not blocktmp2.isGenesis:
            blocktmp1 = blocktmp2.parentblock
            stat.append(blocktmp2.blockhead.timestamp - blocktmp1.blockhead.timestamp)
            blocktmp2 = blocktmp1
        if height <= 1000:
            bins = 10
        else:
            bins = 20
        plt.rcParams['font.size'] = 16
        plt.rcParams['font.family'] = 'Times New Roman'
        # 无产块间隔样本时仍保存原格式的空直方图。
        interval_max = max(stat) if stat else 1
        plt.hist(stat, bins=bins, histtype='bar', range=(0, interval_max))
        plt.xlabel('Rounds')
        plt.ylabel('Times')
        # plt.title('Block generation interval distribution')
        RESULT_PATH = global_var.get_result_path()
        plt.savefig(RESULT_PATH / 'block_interval_distribution.svg')
        if global_var.get_show_fig():
            plt.show()
        plt.close()
    
    def printchain2txt(self, chain_data_url='chain_data.txt'):
        '''
        前向遍历打印链中所有块到文件
        param:
            chain_data_url:打印文件位置,默认'chain_data.txt'
        '''
        def save_chain_structure(chain,f):
            blocklist = [chain.head]
            printnum = 1
            while blocklist:
                length = 0
                print("|    ", end="",file=f)
                print("-|   " * (printnum - 1),file=f)
                while printnum > 0:
                    blocklist.extend(blocklist[0].next)
                    blockprint = blocklist.pop(0)
                    length += len(blockprint.next)
                    print("{}   ".format(blockprint.name), end="",file=f)
                    printnum -= 1
                print("",file=f)
                printnum = length

        CHAIN_DATA_PATH=global_var.get_chain_data_path()
        if not self.head:
            with open(CHAIN_DATA_PATH / chain_data_url,'a') as f:
                print("empty chain",file=f)
            return
        
        with open(CHAIN_DATA_PATH /chain_data_url,'a') as f:
            print("Blockchain maintained BY Miner",self.miner_id,file=f)
            # 打印主链
            save_chain_structure(self,f)
            #打印链信息
            q:list[Block] = [self.head]
            blocklist = []
            while q:
                block = q.pop(0)
                blocklist.append(block)
                print(block,file=f)
                for i in block.next:
                    q.append(i)


    def CalculateStatistics(self, rounds, honest_miners: list, adver_ids: list[int], confirm_delay: int,
                            dataitem_params: dict, valid_dataitems: set, quantile: float,
                            eclipsed_ids: list = None, chain_switch_events: list = None):
        # 旧树遍历统计不能静默用于全DAG；旧模式逐句保留。
        if getattr(self, '_dag_mode', False):
            raise ValueError('DAG statistics require Environment.view / util.dag_metrics.collect_metrics')
        eclipsed_ids = eclipsed_ids or []
        # 统计一些数据
        stats = {
            "num_of_generated_blocks": -1,
            "height_of_longest_chain": 0,
            "num_of_valid_blocks": 0,
            "num_of_stale_blocks": 0,
            "stale_rate": 0,
            "num_of_forks": 0,
            "num_of_heights_with_fork":0,
            "fork_rate": 0,
            "average_block_time_main": 0,
            "block_throughput_main": 0,
            "throughput_main_MB": 0,
            "average_block_time_total": 0,
            "block_throughput_total": 0,
            "throughput_total_MB": 0,
            "double_spending_success_times": 0,
            # "double_spending_success_times_ver2": 0,
            # "double_spending_success_count_by_switch": 0,
            "attack_fail": 0
        }
        q = [self.head]
        while q:
            stats["num_of_generated_blocks"] = stats["num_of_generated_blocks"] + 1
            blocktmp = q.pop(0)
            if blocktmp.height > stats["height_of_longest_chain"]:
                stats["height_of_longest_chain"] = blocktmp.height
            nextlist = blocktmp.next
            q.extend(nextlist)

        from external import common_prefix
        honest_cp = self.last_block
        honest_miner_ids = set(miner.miner_id for miner in honest_miners)
        honest_miner_sorted_by_height = honest_miners.copy()
        honest_miner_sorted_by_height.sort(key=lambda x: x.get_local_chain().get_height(), reverse=True)
        quantile = quantile if quantile > 0 else 1 / len(honest_miners)
        consensus_miner_num = int(len(honest_miners) * quantile)
        consensus_miner_num = max(consensus_miner_num, 1)
        for miner in honest_miner_sorted_by_height[:consensus_miner_num]:
            honest_miner_ids.add(miner.miner_id)
            honest_cp = common_prefix(honest_cp, miner.get_local_chain().get_last_block())
        stats["num_of_valid_blocks"] = honest_cp.height

        # Check the dataitems in the honest_cp
        from external import R                                                                                                                                                                         
        valid_item_count = 0
        anomalous_item_count = 0
        if dataitem_params['dataitem_enable']:
            dataitem_set = set()
            dataitems_reversed = R(honest_cp)
            previous_item = int.from_bytes((255).to_bytes(1, BYTE_ORDER) * 2 * INT_LEN, BYTE_ORDER)
            for dataitem in dataitems_reversed:
                if dataitem.to_bytes(2*INT_LEN, BYTE_ORDER) in valid_dataitems:
                    if previous_item < dataitem and dataitem_params['dataitem_input_interval'] > 0:
                        anomalous_item_count += 1
                        logger.warning("A dataitem is out of order: %s", dataitem)
                    else:
                        valid_item_count += 1
                        previous_item = dataitem
                        if dataitem not in dataitem_set:
                            dataitem_set.add(dataitem)
                        else:
                            logger.warning("A dataitem duplicates")
                else:
                    anomalous_item_count += 1
            stats.update({
                'valid_dataitem_throughput': valid_item_count / rounds,
                'input_dataitem_rate': 1/dataitem_params['dataitem_input_interval'] \
                    if dataitem_params['dataitem_input_interval'] > 0 else valid_item_count / rounds
            })
            if stats['num_of_valid_blocks'] > 0:
                stats['block_average_size'] = \
                    (valid_item_count+anomalous_item_count) * dataitem_params['dataitem_size'] / stats['num_of_valid_blocks']
                stats['valid_dataitem_rate'] = valid_item_count / (valid_item_count + anomalous_item_count)
            else:
                stats['valid_dataitem_rate'] = 0
                stats['block_average_size'] = 0

        mainchain_block = {}
        current_block = honest_cp
        while current_block:
            mainchain_block[current_block.name] = current_block
            current_block = current_block.parentblock

        last_block_iter = honest_cp.parentblock
        while last_block_iter:
            stats["num_of_forks"] += len(last_block_iter.next) - 1
            stats["num_of_heights_with_fork"] += (len(last_block_iter.next) > 1)

            if len(last_block_iter.next) > 1:  # 发生分叉
                attacker_block_on_main = None
                # 1. 确认攻击者的区块在主链上
                for child_block in last_block_iter.next:
                    if (child_block.blockhead.miner in adver_ids or \
                        child_block.blockhead.miner in eclipsed_ids) and \
                            child_block.name in mainchain_block:
                        attacker_block_on_main = child_block
                        break

                if attacker_block_on_main:
                    # 攻击者区块已在主链，现在检查是否有符合条件的被孤立分支
                    found_qualifying_orphaned_branch = False
                    for other_child_block in last_block_iter.next:
                        if other_child_block == attacker_block_on_main:
                            continue  # 跳过攻击者所在的主链分支

                        # 条件1: 被孤立分支的第一个块 (other_child_block) 需要是诚实块
                        if other_child_block.blockhead.miner in honest_miner_ids:  # 确保是被孤立的分支

                            # 条件2: 这个被孤立的分支长度需要大于等于 N
                            # DFS栈: (区块, 从other_child_block开始的当前路径长度)
                            dfs_stack = [(other_child_block, 1)]

                            # 记录这个孤立分支的最长路径
                            # 这个DFS只关心路径长度，不关心路径上后续块的诚实性
                            temp_longest_path_for_orphan = 0

                            while dfs_stack:
                                current_orphan_node, current_path_length = dfs_stack.pop()

                                if current_path_length > temp_longest_path_for_orphan:
                                    temp_longest_path_for_orphan = current_path_length

                                for next_orphan_node_candidate in current_orphan_node.next:
                                    dfs_stack.append((next_orphan_node_candidate, current_path_length + 1))

                            # 现在 temp_longest_path_for_orphan 是这条以诚实块开始的孤立分支的最长长度
                            if temp_longest_path_for_orphan >= confirm_delay:
                                found_qualifying_orphaned_branch = True
                                break  # 找到了一个符合条件的被孤立分支

                    if found_qualifying_orphaned_branch:
                        stats["double_spending_success_times"] += 1

            last_block_iter = last_block_iter.parentblock

        # # 第二种静态计算双花攻击成功的方法
        # last_block = honest_cp
        # attack_flag = False if last_block.blockhead.miner  in honest_miner_ids else True
        # last_block = honest_cp.parentblock
        # while last_block:
        #     if len(last_block.next) > 1 and attack_flag:
        #         attack_flag = False
        #         stats["double_spending_success_times_ver2"] += 1
        #     if last_block.blockhead.miner in adver_ids and not attack_flag:
        #         attack_flag = True
        #     last_block = last_block.parentblock

        # 计算双花攻击成功次数-按照链切换事件
        # honest_chains = [miner.get_local_chain() for miner in honest_miners]
        # stats["double_spending_success_count_by_switch"] = double_spending_success_by_switch(honest_chains, adver_ids,
        #                                                            mainchain_block, confirm_delay, eclipsed_ids,
        #                                                            chain_switch_events, quantile)

        # Chain Quality Property
        from external import chain_quality
        cq_dict, chain_quality_property = chain_quality(honest_cp, adver_ids)
        stats.update({
            'chain_quality_property': cq_dict,
            'ratio_of_blocks_contributed_by_malicious_players': round(chain_quality_property, 5),
            'upper_bound t/(n-t)': round(len(adver_ids) / len(honest_miner_ids), 5)
        })

        stats["num_of_stale_blocks"] = stats["num_of_generated_blocks"] - stats["num_of_valid_blocks"]
        stats["average_block_time_main"] = rounds / stats["num_of_valid_blocks"] if stats["num_of_valid_blocks"] > 0 else -1
        # 无运行轮次时保持原数值键，吞吐输出 0。
        stats["block_throughput_main"] = stats["num_of_valid_blocks"] / rounds if rounds > 0 else 0
        blocksize = global_var.get_blocksize()
        stats["throughput_main_MB"] = blocksize * stats["block_throughput_main"]
        # 无区块时平均间隔沿用 main 的 -1 哨兵。
        # 非空正轮次仍使用原计算公式；0 吞吐是空运行输出约定，不是性能测量。
        stats["average_block_time_total"] = (rounds / stats["num_of_generated_blocks"]
                                            if stats["num_of_generated_blocks"] > 0 else -1)
        stats["block_throughput_total"] = (1 / stats["average_block_time_total"]
                                           if rounds > 0 and stats["num_of_generated_blocks"] > 0 else 0)
        stats["throughput_total_MB"] = blocksize * stats["block_throughput_total"]
        stats["fork_rate"] = stats["num_of_heights_with_fork"] / stats["num_of_valid_blocks"] if stats["num_of_valid_blocks"] > 0 else 0
        # 无产块样本时无 stale，保留原键及数值类型。
        stats["stale_rate"] = (stats["num_of_stale_blocks"] / stats["num_of_generated_blocks"]
                               if stats["num_of_generated_blocks"] > 0 else 0)

        return stats
    
    def set_switch_tracker_callback(self, callback):
        self.switch_tracker_callback = callback

    def set_merge_tracker_callback(self, callback):
        self.merge_tracker_callback = callback

    def set_merge_callback(self, callback):
        self.merge_callback = callback


def double_spending_success_by_switch(honest_chains:list[Chain], adver_ids:list[int], mainchain_block:dict,
                            confirm_delay: int, eclipsed_ids:list[int], chain_switch_events:list,
                            quantile: float):
    from external import common_prefix
    miner_num = global_var.get_miner_num()
    local_chain_tip_name = ['B0'] * miner_num
    local_chain_tip_hash = [honest_chains[0].head] * miner_num
    honest_miner_ids = [chain.miner_id for chain in honest_chains]

    def search_in_global_view(blockhash):
        for chain in honest_chains:
            block = chain.search_block_by_hash(blockhash)
            if block is not None:
                return block
        return None

    chain_override_events = {} # {fork_block: {"block_height", "switching_subjects"}}
    for event in chain_switch_events:
        if event.subject not in honest_miner_ids:
            # 仅关注诚实矿工
            continue
        if local_chain_tip_name[event.subject] not in mainchain_block and event.name in mainchain_block:
            # 从分叉切换到主链
            switch_source_block = search_in_global_view(local_chain_tip_hash[event.subject])
            if switch_source_block is None:
                print(local_chain_tip_name[event.subject])
            switch_target_block = mainchain_block[event.name]
            fork_block = common_prefix(switch_source_block, switch_target_block)
            if switch_source_block.get_height() - fork_block.get_height() >= confirm_delay:
                # 达到N=2的标准
                flag = False
                blocktmp = switch_target_block
                while blocktmp.blockhash != fork_block.blockhash:
                    if blocktmp.blockhead.miner in adver_ids or blocktmp.blockhead.miner in eclipsed_ids:
                        # 主链上有一个攻击者或者被月蚀攻击矿工的块
                        flag = True
                        break
                    blocktmp = blocktmp.parentblock
                if flag:
                    # 认定为一次override事件
                    default_item = {'block_height': fork_block.get_height(), 'switching_subjects': {}}
                    chain_override_events.setdefault(fork_block.name, default_item)
                    chain_override_events[fork_block.name]['switching_subjects'].setdefault(event.subject,[])
                    chain_override_events[fork_block.name]['switching_subjects'][event.subject].append((event.switch_round,
                                                                                                        switch_source_block,
                                                                                                        switch_target_block))                    
        local_chain_tip_name[event.subject] = event.name
        local_chain_tip_hash[event.subject] = event.blockhash
    
    success_history_reanalysis = {} # {fork_block: {"block_height", "switching_subjects"}}
    for fork_block, switch_event in chain_override_events.items():
        switch_history = switch_event['switching_subjects']
        if len(switch_history) >= quantile * len(honest_miner_ids):
            # 认定为攻击成功
            success_history_reanalysis[fork_block] = switch_history

    return len(success_history_reanalysis)


class LocalChainTracker(object):
    '''记录本地链每次切换lastblock的轮次以及lastblock的信息'''
    class ChainSwitchEvent(object):
        '''记录一次链尾切换的事件'''
        __slots__ = ['switch_round','name','blockhash','height','timestamp','subject']
        def __init__(self,switch_round:int,last_block:Block, subject:int):
            self.switch_round = switch_round
            self.name = last_block.name
            self.blockhash = last_block.blockhash
            self.height = last_block.height
            self.timestamp = last_block.blockhead.timestamp
            self.subject = subject # the miner who switches lastblock

    # class ChainMergeEvent(object):
    #     '''记录一次区块并入本地链的事件'''
    #     __slots__ = ['merge_round','name','blockhash','height','timestamp','subject']
    #     def __init__(self,merge_round:int,last_block:Block, subject:int):
    #         self.merge_round = merge_round
    #         self.name = last_block.name
    #         self.blockhash = last_block.blockhash
    #         self.height = last_block.height
    #         self.timestamp = last_block.blockhead.timestamp
    #         self.subject = subject # the miner who switches lastblock

    def __init__(self):
        self.chain_switch_events = []
        # self.block_merge_round = dict()
        self.current_round = 0
        
    def get_switch_tracker(self, miner_id:int):
        def tracker(last_block:Block):
            self.chain_switch_events.append(LocalChainTracker.ChainSwitchEvent(self.current_round,
                                                                                      last_block, miner_id))
        return tracker
    
    # def get_merge_tracker(self, miner_id:int):
    #     block_merge_round_per_miner = dict()
    #     self.block_merge_round[miner_id] = block_merge_round_per_miner
    #     def tracker(last_block:Block):
    #         block_merge_round_per_miner[last_block.blockhash] = self.current_round

    #     return tracker
    
    def update_round(self,current_round):
        self.current_round = current_round

    def dump_dag_events(self, filename, chains, settings):
        """原tracker的DAG输出入口，安全版本化accepted-only格式。

        完整输入边界另存dag_events；旧dump_events的pickle格式和参数完全保留。
        """
        from util.dag_replay import write_accepted_dump
        write_accepted_dump(filename, chains, settings)

    def dump_events(self, filename: str, chains: list[Chain] = None,
                    honest_miner_ids: list[int] = None):
        dump_data = {'chain_switch_events': self.chain_switch_events}
        if chains is not None:
            chain_state = []
            for chain in chains:
                chain_copy = copy.deepcopy(chain)
                chain_state.append(chain_copy)
            dump_data['chains'] = chain_state
        if honest_miner_ids is not None:
            dump_data['honest_miner_ids'] = honest_miner_ids
        '''将链切换事件记录到文件'''
        import pickle
        with open(filename, 'wb') as f:
            pickle.dump(dump_data, f)
