import logging
from array import array

import global_var
from consensus import Consensus
from data import Block, Message
from external import I
from functions import for_name

from ._consts import FLOODING, SPEC_TARGETS, OUTER_RCV_MSG, SELF_GEN_MSG, SYNC_LOC_CHAIN
from .network_interface import NetworkInterface, NICWithoutTp, NICWithTp

logger = logging.getLogger(__name__)


class Miner(object):
    def __init__(self, miner_id, consensus_params:dict, max_block_capacity:int = 0, disable_dataitem_queue=False):
        self.miner_id = miner_id #矿工ID
        self._isAdversary = False
        #共识相关
        self.consensus:Consensus = for_name(
            global_var.get_consensus_type())(miner_id, consensus_params)# 共识
        #输入内容相关
        self.input_tape = []
        self.round = -1
        #网络接口
        self._NIC:NetworkInterface =  None
        self.receive_history = dict()
        # 只读阶段观察，wire 与 accepted 的轮次分开。
        self.dag_enabled = global_var.get_consensus_type() == 'consensus.ConfluxPoW'
        if self.dag_enabled:
            self.dag_wire_rounds = {}
            self.dag_accepted_rounds = {self.get_local_chain().head.blockhash: 0}
            # DAG增量优化：按接纳日志游标观察新增块，不在每个边界扫描历史区块集。
            self._dag_observed_log=self.get_local_chain()._dag_accepted_hashes
            self._dag_observed_count=len(self._dag_observed_log)
            self.dag_state_history = []
            self.dag_protocol_events = []
            # 项级历史独立于当前资格与块历史。
            self.dag_dataitem_first_confirmed = {}
            self.dag_dataitem_confirmation_events = []
            self.dag_publication_observer = None
            self._dag_previous_pivot = self.get_local_chain().protocol_view.pivot

        # maximum data items in a block
        self.max_block_capacity = max_block_capacity
        # DAG只持一份bytes池，旧array池仅legacy实例创建。
        if not self.dag_enabled and self.max_block_capacity > 0 and not disable_dataitem_queue:
            self.dataitem_queue = array('Q')
        # DAG用bytes状态；旧array('Q')路径不改。
        if self.dag_enabled:
            from .conflux_dataitems import ConfluxDataitemState
            self.dag_dataitems = ConfluxDataitemState(not disable_dataitem_queue) if max_block_capacity > 0 else None
            # 零轮也须区分启用空集合与关闭None。
            # 先消费创世view，后同步确认；不制造接纳/确认事件或输入。
            if self.dag_dataitems is not None:
                self.dag_dataitems.synchronize(self.get_local_chain())
                self._observe_dataitem_confirmation(0, 'initial')
            # 初始空状态也须可核对，不增加协议边界。
            from util.dag_audit import annotate_dataitems
            annotate_dataitems(self, 'initial')
        #保存矿工信息
        CHAIN_DATA_PATH=global_var.get_chain_data_path()
        with open(CHAIN_DATA_PATH / f'chain_data{str(self.miner_id)}.txt','a') as f:
            print(f"Miner {self.miner_id}\n"
                  f"consensus_params: {consensus_params}", file=f)
    
    def get_local_chain(self):
        return self.consensus.local_chain
    
    def in_local_chain(self, block:Block):
        return self.consensus.in_local_chain(block)

    def has_received(self, block:Message):
        return self.consensus.has_received(block)

    def _join_network(self, network):
        """初始化网络接口"""
        if network.withTopology:
            self._NIC = NICWithTp(self)
        else:
            self._NIC = NICWithoutTp(self)
        self._NIC.nic_join_network(network)
    
    @property
    def network_has_topology(self):
        """网络是否有拓扑结构"""
        return isinstance(self._NIC, NICWithTp)

    @property
    def neighbors(self):
        return self._NIC._neighbors
        
    def set_adversary(self, _isAdversary:bool):
        '''
        设置是否为对手节点
        _isAdversary=True为对手节点
        '''
        self._isAdversary = _isAdversary

    def receive(self, source:int, msg: Message):
        '''处理接收到的消息，直接调用consensus.receive'''
        rcvSuccess = self.consensus.receive_filter(msg)
        if not rcvSuccess:
            return rcvSuccess
        else:
            self.receive_history[msg.name] = source
            # receive_filter 成功只说明完整消息进入 tape。
            if self.dag_enabled and isinstance(msg, Block):
                self.dag_wire_rounds.setdefault(msg.blockhash, self.round)


        return rcvSuccess
    
       
    def forward(self, msgs:list[Message], msg_source_type, forward_strategy:str=FLOODING, 
                spec_targets:list=None, syncLocalChain = False):
        """将消息转发给其他节点

        args:
            msgs: list[Message] 需要转发的消息列表
            msg_source_type: str 消息来源类型, SELF_GEN_MSG表示由本矿工产生, OUTER_RCV_MSG表示由网络接收
            forward_strategy: str 消息转发策略
            spec_targets: list[int] 如果forward_strategy为SPECIFIC, 则spec_targets为转发的目标节点列表
            syncLocalChain: bool 是否向邻居同步本地链，尽量在产生新区块时同步
        
        """
        if msg_source_type != SELF_GEN_MSG and msg_source_type != OUTER_RCV_MSG:
            raise ValueError("Message type must be SELF or OUTER")
        logger.info("M%d: forwarding %s, type %s, strategy %s", self.miner_id, 
                    str([msg.name for msg in msgs] if len(msgs)>0 else [""]), msg_source_type, forward_strategy)
        for msg in msgs:
            self._NIC.append_forward_buffer(msg, msg_source_type, forward_strategy, spec_targets, syncLocalChain)

    
    def launch_consensus(self, input, round):
        '''开始共识过程

        return:
            new_msg 由共识类产生的新消息，没有就返回None type:list[Message]/None
            msg_available 如果有新的消息产生则为True type:Bool
        '''
        new_msgs, msg_available = self.consensus.consensus_process(
            self._isAdversary,input, round)
        if new_msgs is not None:
            # 自产/本地 accepted 与接收端使用同一大小模型。
            # 在 Environment 记录生成和 NIC 排队之前计算；size/segment_num 不参与头部哈希。
            if self.dag_enabled:
                for msg in new_msgs:
                    if isinstance(msg, Block):
                        self._NIC._network.message_preprocessing(msg)
                        # 记录实际大小，保持头部身份输入不变。
                        from util.dag_audit import refresh_generated_wire_metadata
                        refresh_generated_wire_metadata(self.consensus, msg)
                        local = self.get_local_chain().block_set.get(msg.blockhash)
                        if local is not None and local is not msg:
                            self._NIC._network.message_preprocessing(local)
            # new_msgs.append(Message("testMsg", 1))
            self.forward(new_msgs, SELF_GEN_MSG, syncLocalChain = True)
        # 自产边界及时观察，避免 delta 被下一批覆盖。
        if self.dag_enabled and msg_available:
            self.observe_dag_state(round, 'self_generated')
        return new_msgs, msg_available  # 返回挖出的区块，
        

    def BackboneProtocol(self, round):
        self.round = round  # 当前真实处理轮次
        _, chain_update, forwarded_msgs = self.consensus.local_state_update(round)
        if forwarded_msgs:
            self.forward(forwarded_msgs, OUTER_RCV_MSG)

        # 接收批次的 delta 在下一次自产之前消费。
        if self.dag_enabled:
            self.observe_dag_state(round, 'receive_before_mining')

        input = I(round, self.input_tape)  # I function
        # 全序状态选择内容；legacy沿用旧队列删除逻辑。
        if self.dag_enabled:
            if self.dag_dataitems is not None:
                # 原队列逻辑保留，只记录实际输入。
                self.consensus.audit_item_input(input)
                input = self.dag_dataitems.select(input, self.max_block_capacity)
        elif self.max_block_capacity > 0 and getattr(self, 'dataitem_queue', None) is not None:
            # exclude dataitems in updated blocks
            if len(chain_update) > 0:
                dataitem_exclude = set()
                for block in chain_update:
                    block:Consensus.Block
                    dataitem_exclude.update(array('Q', block.blockhead.content))
                self.dataitem_queue = array('Q', [x for x in self.dataitem_queue if x not in dataitem_exclude])
            self.dataitem_queue.frombytes(input)
            if len(self.dataitem_queue) > 10 * self.max_block_capacity:
                # drop the oldest data items if the queue is longer than 2 * max_block_capacity
                self.dataitem_queue.pop(0)
            input = self.dataitem_queue[:self.max_block_capacity].tobytes()

        new_msgs, msg_available = self.launch_consensus(input, round)

        if msg_available:
            # remove the data items in the new block from dataitem_queue
            # DAG自产是否退出池由已消费order决定，不能再次删capacity项。
            if not self.dag_enabled and self.max_block_capacity > 0 and getattr(self, 'dataitem_queue', None) is not None:
                self.dataitem_queue = self.dataitem_queue[self.max_block_capacity:]
            return new_msgs
        return None  #  如果没有更新 返回空告诉environment回合结束
        
    
    def clear_tapes(self):
        # clear the input tape
        self.input_tape = []
        # clear the communication tape
        self.consensus.receive_tape = []
        self._NIC._receive_buffer.clear()
        # self._NIC.clear_forward_buffer()
    
        


    # 复用原 local_state_update，排空不调用 launch_consensus。
    def process_received(self, round, phase='receive_end_round'):
        """完整到达批次入图/排序/转发，再清 tape；不挖矿、不制造 input。"""
        self.round = round
        _, _, forwarded = self.consensus.local_state_update(round)
        if forwarded:
            self.forward(forwarded, OUTER_RCV_MSG)
        self.observe_dag_state(round, phase)
        self.clear_tapes()

    def observe_dag_state(self, round, phase):
        """记录首次接纳与协议差量；added/removed是重排后缀而不是产块数。

        仅记录本节点状态；不把环境或其他节点图作为模板依赖。节点输入日志与声明差量分开，离线重放必须重新验证。
        """
        chain = self.get_local_chain()
        # 每次接收/自产/排空观察都消费本节点协议差量。
        if self.dag_dataitems is not None:
            self.dag_dataitems.synchronize(chain)
            # 不依赖order变化，单独同步确认前缀。
            self._observe_dataitem_confirmation(round, phase)
        accepted_log=chain._dag_accepted_hashes
        if getattr(self,'_dag_observed_log',None) is accepted_log:
            newly_accepted=sorted(accepted_log[self._dag_observed_count:])
        else:
            # 仅显式替换/恢复本地图时重新对齐游标，保持首次接纳时间语义。
            newly_accepted=sorted(h for h in accepted_log if h not in self.dag_accepted_rounds)
        for blockhash in newly_accepted:
            # 恢复旧快照后再次接纳同一hash，仍保留历史首次接纳时间。
            self.dag_accepted_rounds.setdefault(blockhash, round)
        self._dag_observed_log,self._dag_observed_count=accepted_log,len(accepted_log)
        # 协议phase与Miner观察phase分别保存。
        from util.dag_audit import annotate_dataitems
        annotate_dataitems(self, phase)
        view, delta = chain.protocol_view, chain.last_protocol_delta
        if delta is not None and (delta.removed or delta.added):
            self.dag_protocol_events.append({
                'round': round, 'phase': phase,
                'sequence': self.consensus.confirmation_tracker.sequence,  # 本地边界序号
                'common_prefix_length': delta.common_prefix_length,
                'removed': [h.hex() for h in delta.removed], 'added': [h.hex() for h in delta.added],
                'pivot_changed': view.pivot != self._dag_previous_pivot})
            # 事件附数据项状态计数，仍非完整输入重放。
            if self.dag_dataitems is not None:
                self.dag_protocol_events[-1]['dataitems'] = self.dag_dataitems.summary()
        self._dag_previous_pivot = view.pivot
        self.dag_state_history.append({'round': round, 'phase': phase,
            'accepted': len(chain.block_set)-1, 'pending': len(self.consensus.dependency_resolver.pending),
            'tips': len(chain._dag_tips), 'pivot_height': chain.get_height(),
            # DAG增量优化：rank只在接纳时增长；pending不进入此最大值。
            'max_dag_rank': chain._dag_max_rank,
            'ordered': len(view.order)-1,
            # 关闭政策为None，开启但无样本为0。
            'confirmed': max(0,len(self.consensus.get_confirmed_order())-1)
                if self.consensus.confirmation_tracker.policy!='none' else None})

    def _observe_dataitem_confirmation(self, round, phase):
        """当前唯一项可撤销，历史首次点不覆盖。"""
        state, tracker = self.dag_dataitems, self.consensus.confirmation_tracker
        old = state.confirmed or ()
        state.set_confirmed_order(tracker.current_order if tracker.policy!='none' else None)
        new = state.confirmed or ()
        gained = tuple(x for x in new if x not in set(old))
        lost = tuple(x for x in old if x not in set(new))
        point = dict(round=round, phase=phase, sequence=tracker.sequence)
        reconfirmed = [x.hex() for x in gained if x in self.dag_dataitem_first_confirmed]
        for item in gained:
            self.dag_dataitem_first_confirmed.setdefault(item, dict(point))
        if gained or lost:
            self.dag_dataitem_confirmation_events.append(dict(point,
                revoked=[x.hex() for x in lost], newly_confirmed=[x.hex() for x in gained],
                reconfirmed=reconfirmed))
