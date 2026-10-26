import copy
import logging
import math
import random
import time
from array import array

import numpy as np

import consensus
import global_var
import network
from data import Block, BlockCarrier, Chain, LocalChainTracker
from external import chain_growth, chain_quality, common_prefix, MAX_SUFFIX
from functions import for_name, INT_LEN, BYTE_ORDER
from miner import Miner, network_interface
from attack.adversary import Adversary

logger = logging.getLogger(__name__)

MAX_INT = 2**32-1

class Environment(object):

    def __init__(self,  attack_param: dict = None,
                 consensus_param:dict = None, network_param:dict = None, 
                 genesis_blockheadextra:dict = None, genesis_blockextra:dict = None,
                 dataitem_param:dict = None):
        '''initiate the running environment

        Param
        -----
        attack_param: attack parameters (dict)
        consensus_param: consensus parameters (dict)
        network_param: network parameters (dict)
        genesis_blockheadextra: initialize variables in the head of genesis block (dict)
        genesis_blockextra: initialize variables in the genesis block (dict)
        dataitem_param: data item parameters (dict) 
        '''
        #environment parameters
        self.miner_num = global_var.get_miner_num()
        self.total_round = 0
        # 模式分流保留原 exec/view 和旧单链路径。
        self.dag_enabled = global_var.get_consensus_type() == 'consensus.ConfluxPoW'
        if self.dag_enabled:
            self.dag_settings = dict(consensus_param['dag_settings'])
            self.consensus_param = copy.deepcopy(consensus_param)
            self.network_param = copy.deepcopy(network_param)
            self.generated_rounds = {}
            self.first_published_rounds = {}
            # 首次实际发送时冻结非生产者诚实cohort。
            self.publication_cohorts = {}
            self.drain_rounds_executed = 0
            self.stop_reason = 'round_limit'
            self.requested_rounds = 0
            self.requested_max_height = 0
            self.ordered_at_mining_end = 0  # 排空前当前序计数，吞吐分子冻结
            self.common_order_at_mining_end = 0  # 新增：全节点共同前缀LD，不依赖输出模式
        # load data item settings
        # enable且容量>0才有效；原参数声明与effective状态分开。
        self.dag_dataitems_active = self.dag_enabled and dataitem_param['dataitem_enable'] and dataitem_param['max_block_capacity'] > 0
        if self.dag_enabled:
            consensus_param['dataitem_settings'] = dict(dataitem_param, active=self.dag_dataitems_active)
            self.consensus_param = copy.deepcopy(consensus_param)
            self.ordered_dataitems_at_mining_end = 0
            self.valid_dataitems_at_mining_end = 0
            # 关闭政策输出None；开启则冻结挖矿结束确认数。
            enabled = self.dag_settings.get('confirmation_policy','none') != 'none'
            self.confirmed_at_mining_end = 0 if enabled else None
            self.confirmed_dataitems_at_mining_end = 0 if enabled and self.dag_dataitems_active else None
        self.dataitem_params = dataitem_param
        if not dataitem_param['dataitem_enable']:
            self.dataitem_validator = None
            # DAG保存容量声明，仅effective关闭；legacy保持原参数处理。
            if not self.dag_enabled:
                self.dataitem_params['max_block_capacity'] = 0
        else:
            self.dataitem_validator = set()
            if dataitem_param['dataitem_input_interval'] == 0:
                initial_dataitems = b''.join([self.generate_dataitem(1) for _ in range(dataitem_param['max_block_capacity'])])
                self.input_dataitem = array('Q', initial_dataitems).tobytes()
                self.new_block_this_round:list[Block] = []
        # configure extra genesis block info
        consensus_type_str = global_var.get_consensus_type()
        consensus_type:consensus.Consensus = for_name(consensus_type_str)
        consensus_type.genesis_blockheadextra = genesis_blockheadextra
        consensus_type.genesis_blockextra = genesis_blockextra
        # Local chain tracker generation
        self.local_chain_tracker = LocalChainTracker()
        # generate miners
        self.miners:list[Miner] = []
        for miner_id in range(self.miner_num):
            # DAG显式传实际有效容量，不丢弃manifest中的原始声明。
            capacity = (dataitem_param['max_block_capacity'] if self.dag_dataitems_active else 0) if self.dag_enabled else dataitem_param['max_block_capacity']
            miner = Miner(miner_id, consensus_param, capacity, dataitem_param['dataitem_input_interval'] == 0)
            miner.get_local_chain().set_switch_tracker_callback(self.local_chain_tracker.get_switch_tracker(miner_id))
            # miner.get_local_chain().set_merge_tracker_callback(self.local_chain_tracker.get_merge_tracker(miner_id))
            self.miners.append(miner)
        self.envir_create_global_chain()

        # generate network
        self.network:network.Network = for_name(global_var.get_network_type())(self.miners)

        # 诚实 DAG 不创建原攻击聚合共识 miner=-1。
        if self.dag_enabled:
            self.adversary = None
            adversary_ids = []
            self.honest_miner_ids = list(range(self.miner_num))
            self.reference_miner_id = min(self.honest_miner_ids)
            for miner in self.miners:
                miner.dag_publication_observer = self.record_first_publication
        else:
            # generate adversary
            self.adversary = Adversary(
                adver_num = attack_param['adver_num'],
                attack_type = attack_param['attack_type'],
                adversary_ids = attack_param['adversary_ids'],
                network_type = self.network,
                consensus_type = global_var.get_consensus_type(),
                miner_list = self.miners,
                # eclipse = attack_param['eclipse'],
                global_chain = self.global_chain,
                adver_consensus_param = copy.deepcopy(consensus_param),
                attack_arg = attack_param['attack_arg'])
            # get honest miner IDs
            adversary_ids = self.adversary.get_adver_ids()
            self.honest_miner_ids = [miner.miner_id for miner in self.miners if miner.miner_id not in adversary_ids]
        self.confirm_delay = consensus_param['N']

        # configure oracles
        if consensus_type_str == 'consensus.SolidPoW':
            self.configure_oracles(consensus_param, adversary_ids)

        # set parameters for network
        self.network.set_net_param(**network_param)
        # 容量0时网络也关闭数据项计费；旧分支保持原行为。
        if (self.dag_dataitems_active if self.dag_enabled else dataitem_param['dataitem_enable']):
            self.network._dataitem_param = dataitem_param
        elif not self.dag_enabled:
            self.adversary.get_attack_type().behavior.ATTACKER_INPUT = None

        # add a line in chain data to distinguish adversaries from non-adversaries
        CHAIN_DATA_PATH=global_var.get_chain_data_path()
        for miner in self.miners:
            with open(CHAIN_DATA_PATH / f'chain_data{str(miner.miner_id)}.txt','a') as f:
                print(f"_isAdversary: {miner._isAdversary}\n", file=f)
        
        from reprlib import repr
        network_param_display = network_param.copy()
        if 'stat_prop_times' in network_param_display:
            network_param_display['stat_prop_times'] = repr(network_param_display['stat_prop_times'])
        if 'prop_vector' in network_param_display:
            network_param_display['prop_vector'] = repr(network_param_display['prop_vector'])
        consensus_param_display = consensus_param.copy()
        if 'q_distr' in consensus_param_display:
            consensus_param_display['q_distr'] = repr(consensus_param_display['q_distr']).strip("'")
        if 'target' in consensus_param_display:
            consensus_param_display['target'] = repr(consensus_param_display['target']).strip("'")
        parameter_str = ('Parameters:\n' + 
            f'Miner Number: {self.miner_num} \n' + 
            f'Consensus Protocol: {consensus_type.__name__} \n' + 
            f'Network Type: {type(self.network).__name__} \n' + 
            f'Network Param:  {network_param_display} \n' + 
            f'Consensus Param: {consensus_param_display} \n')
        if adversary_ids:
            parameter_str += f'Adversary Miners: {adversary_ids} \n'
            parameter_str += f'Attack Execute Type: {self.adversary.get_attack_type_name()}'
            # parameter_str += f'  (Eclipse: {self.adversary.get_eclipse()}) \n'
            parameter_str += f"  (Adversary's q: {self.adversary.get_adver_q()}) \n"
        if isinstance(self.miners[0]._NIC, network_interface.NICWithTp):
            if self.dataitem_params['dataitem_enable']:
                parameter_str += f'Dataitem Param: {dataitem_param} \n'
            else:
                parameter_str += f'Block Size: {global_var.get_blocksize()} \n'
        # 附加 DAG 身份与未启用参数，不改变旧文本。
        if self.dag_enabled:
            parameter_str += f'DAG Settings: {self.dag_settings} \n'
            # 原N字段按政策明确是否有效。
            if self.dag_settings['confirmation_policy'] == 'none':
                parameter_str += f'Reference Miner: {self.reference_miner_id}; N retained inactive (confirmation_policy=none) \n'
            else:
                parameter_str += f'Reference Miner: {self.reference_miner_id}; N={self.confirm_delay} (pivot_depth, reversible empirical policy) \n'
        print(parameter_str)
        with open(global_var.get_result_path() / 'parameters.txt', 'w+') as conf:
            print(parameter_str, file=conf)

    def configure_oracles(self, consensus_params:dict, adversary_ids:list):
        if consensus_params['q_distr'] == 'equal':
            q_list = [consensus_params['q_ave'] for _ in range(self.miner_num)]
        elif isinstance(consensus_params['q_distr'], list):
            q_list = consensus_params['q_distr']
        else:   
            raise ValueError("q_distr should be a list or the string 'equal'")

        from consensus import RandomOracleRoot
        self.oracle_root = RandomOracleRoot()
        self.verifying_oracles = []
        self.mining_oracles = []
        attacker_oracle_verifying = self.oracle_root.get_verifying_oracle(adversary_ids)
        adversary_q = [q_list[id] for id in adversary_ids]
        attacker_oracle_mining = self.oracle_root.get_mining_oracle(sum(adversary_q))

        for id, miner in enumerate(self.miners):
            if id in adversary_ids:
                verifying_oracle = attacker_oracle_verifying
                mining_oracle = attacker_oracle_mining
            else:
                verifying_oracle = self.oracle_root.get_verifying_oracle([miner.miner_id])
                mining_oracle = self.oracle_root.get_mining_oracle(q_list[miner.miner_id])

            self.verifying_oracles.append(verifying_oracle)
            self.mining_oracles.append(mining_oracle)
            miner.consensus.set_random_oracle(mining_oracle=mining_oracle, verifying_oracle=verifying_oracle)

        self.adversary.get_attack_type().adver_consensus.set_random_oracle(mining_oracle=attacker_oracle_mining,
                                                                           verifying_oracle=attacker_oracle_verifying)

    def envir_create_global_chain(self):
        '''create global chain and its genesis block by copying
          local chain from the first miner.'''
        # 全知生成记录图只用于评估；矿工从不查询它。
        if self.dag_enabled:
            self.global_chain = copy.deepcopy(self.miners[0].get_local_chain())
            self.global_chain.set_switch_tracker_callback(None)
            return
        self.global_chain = Chain()
        self.global_chain.add_blocks(blocks=copy.deepcopy(self.miners[0].consensus.local_chain.head))
        # self.global_chain.head = copy.deepcopy(self.miners[0].consensus.local_chain.head)
        # self.global_chain.lastblock = self.global_chain.head
        if getattr(self, 'new_block_this_round', None) is not None:
            self.global_chain.set_merge_callback(lambda block: self.new_block_this_round.append(block))

    def generate_dataitem(self, round):
        dataitem = random.randint(1, MAX_INT).to_bytes(INT_LEN, BYTE_ORDER)
        dataitem += round.to_bytes(INT_LEN, BYTE_ORDER)
        self.dataitem_validator.add(dataitem)
        return dataitem        

    def on_round_start(self, round):
        self.local_chain_tracker.update_round(round)
        if getattr(self, 'oracle_root', None):
            self.oracle_root.reset_counters(self.mining_oracles)
        # DAG抽象输入仍在原Environment产生；不影响旧单链算法。
        if self.dag_enabled:
            self._on_conflux_dataitem_input(round)
            return
        if self.dataitem_params['dataitem_enable']:
            if self.dataitem_params['dataitem_input_interval'] > 0:
                if round % self.dataitem_params['dataitem_input_interval'] == 1:
                    self.input_dataitem = self.generate_dataitem(round)
                else:
                    self.input_dataitem = b''
            elif len(self.new_block_this_round) > 0: # update global dataitem queue
                dataitems_exclude = set()
                for block in self.new_block_this_round:
                    dataitems_exclude.update(array('Q', block.blockhead.content))
                original_dataitems = array('Q', self.input_dataitem)
                new_dataitems = []
                for item in original_dataitems:
                    if item not in dataitems_exclude:
                        new_dataitems.append(item)
                generate_num = self.dataitem_params['max_block_capacity'] - len(new_dataitems)
                new_dataitems = [array('Q', new_dataitems).tobytes()]
                new_dataitems.extend([self.generate_dataitem(round) for _ in range(generate_num)])
                self.input_dataitem = b''.join(new_dataitems)
                self.new_block_this_round = []
        else:
            self.input_dataitem = round.to_bytes(INT_LEN, BYTE_ORDER)

    # 原dataitem两种输入政策的DAG适配。
    def _on_conflux_dataitem_input(self, round):
        """间隔>0从第1轮定期INSERT；间隔0按上轮全局实际生成块补充全知池。

        后者保留原平台假设：任意生产者打包即从全局输入池替换，不依赖公开order/确认；
        仅提供应用输入，不向节点泄露区块/引用/共识状态。
        """
        if not self.dag_dataitems_active:
            self.input_dataitem = round.to_bytes(INT_LEN, BYTE_ORDER)
            return
        from external import split_dataitems
        interval = self.dataitem_params['dataitem_input_interval']
        if interval > 0:
            self.input_dataitem = self.generate_dataitem(round) if (round-1) % interval == 0 else b''
        elif self.new_block_this_round:
            packed = {x for b in self.new_block_this_round for x in split_dataitems(b.blockhead.content)}
            remaining = [x for x in split_dataitems(self.input_dataitem) if x not in packed]
            missing = self.dataitem_params['max_block_capacity']-len(remaining)
            self.input_dataitem = b''.join(remaining + [self.generate_dataitem(round) for _ in range(missing)])
            self.new_block_this_round = []

    def _freeze_conflux_dataitems(self):
        """停止挖矿时冻结唯一ordered及有效输入项数；排空后的新观察分列。"""
        if self.dag_dataitems_active:
            from external import read_ordered_dataitems
            items = read_ordered_dataitems(self.miners[self.reference_miner_id].get_local_chain())
            self.ordered_dataitems_at_mining_end = len(items)
            self.valid_dataitems_at_mining_end = len(set(items) & self.dataitem_validator)

    def _freeze_conflux_confirmation(self):
        """分母仍为挖矿轮数，迟到确认只计入after_drain。"""
        if self.dag_settings['confirmation_policy'] != 'none':
            miner = self.miners[self.reference_miner_id]
            self.confirmed_at_mining_end = max(0,len(miner.consensus.get_confirmed_order())-1)
            if self.dag_dataitems_active:
                self.confirmed_dataitems_at_mining_end = len(miner.dag_dataitems.confirmed or ())

    def post_verification(self):
        '''post verification of the longest chain in the global_chain'''
        # 检查全生成图及每个节点全图，包括未排序旁支。
        if self.dag_enabled:
            validator = self.miners[self.reference_miner_id].consensus
            return (validator.valid_dag(self.global_chain)
                    and all(miner.consensus.valid_dag(miner.get_local_chain()) for miner in self.miners))
        # construct consensus object with no verification limit
        consensus_obj:consensus.Consensus = copy.copy(self.miners[self.honest_miner_ids[0]].consensus)
        consensus_obj.miner_id = -1
        consensus_name = type(consensus_obj).__name__
        if consensus_name == 'SolidPoW':
            consensus_obj.set_random_oracle(None, self.oracle_root.get_verifying_oracle([]))
        # check the longest chain in the global chain
        longest_chain = self.global_chain.get_last_block()
        # check if the longest chain is valid
        if not consensus_obj.valid_chain(longest_chain):
            return False
        else:
            return True

    def exec(self, num_rounds, max_height, process_bar_type, post_verification):

        '''
        调用当前miner的BackboneProtocol完成mining
        当前miner用add_block_direct添加上链
        之后gobal_chain用深拷贝的add_block_copy上链
        '''
        # 同一原生入口，DAG 使用全图插入和接收批末。
        if self.dag_enabled:
            return self._exec_conflux(num_rounds, max_height, process_bar_type, post_verification)
        if process_bar_type != 'round' and process_bar_type != 'height':
            raise ValueError('process_bar_type should be \'round\' or \'height\'')
        ## 开始循环
        t_0 = time.time() # 记录起始时间
        cached_height = self.global_chain.get_height()
        # 零轮时没有循环赋值，也必须保存 total_round=0。
        round = 0
        try:
            for round in range(1, num_rounds+1):
                self.on_round_start(round)
                inputfromz = self.input_dataitem # 生成输入

                adver_tmpflag = 1
                if self.adversary.get_adver_num() != 0:
                    adverflag = random.randint(1,self.adversary.get_adver_num())
                
                for miner in self.miners:
                    miner.input_tape.append(("INSERT", inputfromz))
                    
                    # 攻击者
                    if miner._isAdversary:
                        if adver_tmpflag == adverflag:
                            self.adversary.excute_per_round(round = round)
                        adver_tmpflag = adver_tmpflag + 1
                        continue
                    
                    # 诚实矿工 run backbone protocol
                    if (new_msgs := miner.BackboneProtocol(round)) is not None:
                        # self.network.access_network(new_msgs,temp_miner.miner_id,round)
                        for msg in new_msgs:
                            if isinstance(msg, Block):
                                self.global_chain.add_block_forcibly(msg)
                            if isinstance(msg, BlockCarrier) and (block := msg.get_block()) is not None:
                                self.global_chain.add_block_forcibly(block)
                    miner.clear_tapes()
                    
                # diffuse(C)
                self.network.diffuse(round)
            
                # 全局链高度超过max_height之后就提前停止
                current_height = self.global_chain.get_height()
                if current_height > max_height:
                    break
                # 根据process_bar_type决定进度条的显示方式
                if process_bar_type == 'round':
                    self.process_bar(round, num_rounds, t_0, 'round/s')
                elif current_height > cached_height and process_bar_type == 'height':
                    cached_height = current_height
                    self.process_bar(current_height, max_height, t_0, 'block/s')
        except KeyboardInterrupt:
            print("\nSimulation interrupted by user, saving existing results...")
        self.total_round = round
        assert post_verification or self.post_verification(), "Post verification failed"
        print(f"\nSimulation finished, global chain reach block height {self.global_chain.get_height()} in {round} rounds")

    def assess_common_prefix(self, type:str = 'cdf'):
        def assess_common_prefix_pdf_per_round(self:Environment, miner_local_chain_tip:list[Block], longest_honest_chain:Block):
            # Common Prefix Property
            # 以全局链为标准视角 与其他矿工的链视角对比

            # 每轮结束时，各个矿工的链与common prefix相差区块个数的分布
            cp_pdf = np.zeros((1, MAX_SUFFIX))

            cp = longest_honest_chain
            for i in self.honest_miner_ids:
                cp = common_prefix(cp, miner_local_chain_tip[i])
            len_cp = cp.get_height()
            for i in range(0, self.miner_num):
                len_suffix = miner_local_chain_tip[i].get_height() - len_cp
                if len_suffix >= 0 and len_suffix < MAX_SUFFIX:
                    cp_pdf[0, len_suffix] = cp_pdf[0, len_suffix] + 1
            return cp_pdf

        def assess_common_prefix_cdf_per_round(self:Environment, miner_local_chain_tip:list[Block], longest_honest_chain:Block):
            # 一种新的计算common prefix的方法，更加接近Bitcoin backbone
            # 每轮结束后，砍掉主链后
            cp_cdf_k = np.zeros((1, MAX_SUFFIX))
            cp_k = longest_honest_chain
            cp_stat = np.zeros((1, self.miner_num))
            for k in range(MAX_SUFFIX):
                # 当所有矿工的链都达标后，后面的都不用算了，降低计算复杂度
                if cp_k is None or np.sum(cp_stat) == len(self.honest_miner_ids):  
                    cp_cdf_k[0, k] += self.miner_num-self.adversary.get_adver_num()
                    continue
                cp_sum_k = 0
                for i in self.honest_miner_ids:
                    if cp_stat[0, i] == 1:
                        cp_sum_k += 1
                        continue
                    if cp_k == common_prefix(cp_k, miner_local_chain_tip[i]):
                        cp_stat[0, i] = 1
                        cp_sum_k += 1
                cp_cdf_k[0, k] += cp_sum_k
                cp_k = cp_k.parentblock
            return cp_cdf_k
        
        if type == 'pdf':
            assess_cp_per_round = assess_common_prefix_pdf_per_round
            cp_xdf_update = np.zeros((1, MAX_SUFFIX))
            cp_xdf_update[0, 0] = self.miner_num
        elif type == 'cdf':
            assess_cp_per_round = assess_common_prefix_cdf_per_round
            cp_xdf_update = np.ones((1, MAX_SUFFIX)) * len(self.honest_miner_ids)
        else:
            raise ValueError('type should be pdf or cdf')

        cp_xdf = np.zeros((1, MAX_SUFFIX))
        # 没有链尾切换时，创世共同前缀保持整个运行。
        # 仅补空事件边界，不改原非空事件的累计算法。
        if not self.local_chain_tracker.chain_switch_events:
            return self.total_round * cp_xdf_update
        # 跟踪矿工本地链链尾
        miner_local_chain_tip = [miner.get_local_chain().head for miner in self.miners]

        event_collector = self.collect_until_next_round() # 收集下一个存在事件的轮次上的所有事件
        previous_round = 0
        while current_events := next(event_collector, None):
            rounds_elapsed = current_events[0].switch_round - previous_round
            previous_round = current_events[0].switch_round
            cp_xdf += rounds_elapsed * cp_xdf_update # 重复利用上一个事件的结果
            
            for event in current_events:
                miner_local_chain_tip[event.subject] = self.miners[event.subject].get_local_chain().search_block_by_hash(event.blockhash)
            honest_chain_tip = [tip for miner, tip in enumerate(miner_local_chain_tip) if miner in self.honest_miner_ids]
            longest_honest_chain = max(honest_chain_tip, key=lambda x: x.get_height())
            cp_xdf_update = assess_cp_per_round(self, miner_local_chain_tip, longest_honest_chain)
        return cp_xdf
        
    def collect_until_next_round(self):
        # 空事件流正常结束，不访问 events[0]。
        if not self.local_chain_tracker.chain_switch_events:
            return
        event_iter = iter(self.local_chain_tracker.chain_switch_events)
        current_round = self.local_chain_tracker.chain_switch_events[0].switch_round
        events = []
        try:
            while True:
                event:LocalChainTracker.ChainSwitchEvent = next(event_iter)
                if event.switch_round > current_round:
                    yield events
                    current_round = event.switch_round
                    events = []
                elif event.switch_round < current_round:
                    raise ValueError('Events are not stored incrementally, something is wrong')
                events.append(event)
        except StopIteration:
            yield events
            return

    def view(self, quantile) -> dict:
        # 纯快照统计，重复 view 不改变原始传播计数。
        if self.dag_enabled:
            from util.dag_metrics import collect_metrics
            self.consensus_miner_quantile = quantile
            stats = collect_metrics(self)
            print('\nChain Growth Property (Conflux local DAG):')
            print(stats['generated'], 'blocks are generated in', self.total_round, 'mining rounds.')
            print('First published:', stats['first_published'], 'Reference accepted:', stats['accepted'],
                  'ordered:', stats['ordered'], 'confirmed:', stats['confirmed'])
            print('Reference pivot height:', stats['pivot_height'], 'tips:', stats['tips'])
            print('Block throughput (ordered):', stats['block_throughput_ordered'], 'blocks/round')
            # 原终端输出追加政策与可撤销确认指标。
            if stats['confirmation_policy'] != 'none':
                print('Confirmation policy:', stats['confirmation_policy'], 'N:', stats['confirmation_N'])
                print('Block throughput (confirmed):', stats['block_throughput_confirmed'], 'blocks/round')
                print('Confirmation revocations:', stats['confirmation_revoked_blocks'], 'block membership losses')
            print('Block propagation times (original wire metric):', stats['block_propagation_times'])
            print('Unfinished network packets:', stats['unfinished_network_packets'])
            print('Invalid complete block inputs:', stats['invalid'], 'Pending dependencies:', stats['pending'])
            print('Honest-recipient accepted thresholds:', stats['propagation_cohorts']['accepted'])
            # 原数据项输出名保留，口径明确为临时全序唯一项。
            if self.dag_dataitems_active:
                print('Ordered unique dataitems:', stats['ordered_dataitems'], 'confirmed:', stats['confirmed_dataitems'])
                print('Throughput of valid dataitems:', stats['valid_dataitem_throughput'], 'items/round')
                print('Throughput of valid dataitems in MB:', stats['valid_dataitem_throughput']*self.dataitem_params['dataitem_size'] if stats['valid_dataitem_throughput'] is not None else None, 'MB/round')
                print('Input dataitem rate:', stats['input_dataitem_rate'], 'items/round')
                print('Average dataitems per block:', stats['average_dataitems_per_ordered_block'], 'items/block')
            return stats
        # 展示一些仿真结果
        print('\n')
        # print("Global Tree Structure:", "")
        # self.global_chain.ShowStructure1()
        # print("End of Global Tree", "")

        # Evaluation Results
        honest_miners = filter(lambda x: x.miner_id in self.honest_miner_ids, self.miners)
        stats = self.global_chain.CalculateStatistics(self.total_round, list(honest_miners), self.adversary.get_adver_ids(),
                                                      self.confirm_delay, self.dataitem_params, self.dataitem_validator, quantile,
                                                      self.adversary.get_eclipsed_ids(), self.local_chain_tracker.chain_switch_events)
        stats.update({'total_round':self.total_round})
        # Chain Growth Property
        growth = 0
        num_honest = 0
        for i in range(self.miner_num):
            if not self.miners[i]._isAdversary:
                growth = growth + chain_growth(self.miners[i].consensus.local_chain)
                num_honest = num_honest + 1
        growth = growth / num_honest
        stats.update({
            'average_chain_growth_in_honest_miners\'_chain': growth
        })
        # Common Prefix Property
        if global_var.get_common_prefix_enable():
            # 零轮没有概率观测，保留键并输出 None。
            if self.total_round == 0:
                stats.update({'common_prefix_pdf': None, 'consistency_rate': None,
                              'common_prefix_cdf_k': None})
            else:
                cp_pdf = self.assess_common_prefix('pdf')
                cp_cdf_k = self.assess_common_prefix('cdf')
                timestamp_of_last_block = self.global_chain.get_last_block().blockhead.timestamp
                # 仅创世时 timestamp=0，用真实运行轮数。
                # 已产块场景继续沿用原归一化分母。
                observation_rounds = timestamp_of_last_block or self.total_round
                stats.update({
                    'common_prefix_pdf': cp_pdf/cp_pdf.sum(),
                    'consistency_rate':cp_pdf[0,0]/(cp_pdf.sum()),
                    'common_prefix_cdf_k': cp_cdf_k/(len(self.honest_miner_ids)*observation_rounds)
                })

        # Network Property
        stats.update({'block_propagation_times': {} })
        if not isinstance(self.network,network.LockstepNetwork):
            self.network: network.StochPropNetwork
            ave_block_propagation_times = self.network.cal_block_propagation_times()
            stats.update({
                'block_propagation_times': ave_block_propagation_times
            })
        inv_count = 0
        sync_full_chain_count = 0
        send_data_count = 0
        for miner in self.miners:
            inv_count += miner._NIC.inv_count
            sync_full_chain_count += miner._NIC.sync_full_chain_count
            send_data_count += miner._NIC.send_data_count
        stats.update({
            'inv_count': inv_count,
            'sync_full_chain_count': sync_full_chain_count,
            'send_data_count': send_data_count
        })
        
        for k,v in stats.items():
            if type(v) is float:
                stats.update({k:round(v,8)})

        # show the results in the terminal
        # Chain Growth Property
        print('Chain Growth Property:')
        print(stats["num_of_generated_blocks"], "blocks are generated in",
              self.total_round, "rounds, in which", stats["num_of_stale_blocks"], "are stale blocks.")
        print("Average chain growth in honest miners' chain:", round(growth, 3))
        print("Number of Forks:", stats["num_of_forks"])
        print("Fork rate:", stats["fork_rate"])
        print("Stale rate:", stats["stale_rate"])
        print("Average block time (main chain):", stats["average_block_time_main"], "rounds/block")
        print("Average block time (total):", stats["average_block_time_total"], "rounds/block")
        print("Block throughput (main chain):", stats["block_throughput_main"], "blocks/round")
        if self.dataitem_params['dataitem_enable']:
            print("Throughput of valid dataitems:", stats['valid_dataitem_throughput'], "items/round")
            print("Throughput of valid dataitems in MB:", 
                stats['valid_dataitem_throughput']*self.dataitem_params['dataitem_size'], "MB/round")
            print("Input dataitem rate:", stats["input_dataitem_rate"], "items/round")
            print("Average dataitems per block:", stats["block_average_size"], "items/block")
        else:
            print("Throughput in MB (main chain):", stats["throughput_main_MB"], "MB/round")
            print("Block throughput (total):", stats["block_throughput_total"], "blocks/round")
            print("Throughput in MB (total):", stats["throughput_total_MB"], "MB/round")
        # Common Prefix Property
        if global_var.get_common_prefix_enable():
            print('Common Prefix Property:')
            print('The common prefix pdf:')
            print(stats["common_prefix_pdf"])
            print('Consistency rate:', stats["consistency_rate"])
            print('The common prefix cdf with respect to k:')
            print(stats["common_prefix_cdf_k"])
        print("")
        # Chain Quality Property
        print('Chain_Quality Property:', stats["chain_quality_property"])
        print('Ratio of blocks contributed by malicious players:', stats["ratio_of_blocks_contributed_by_malicious_players"])
        if self.dataitem_params['dataitem_enable']:
            print('Ratio of dataitems contributed by malicious players:', 1 - stats["valid_dataitem_rate"])
        # Attack Property
        if self.adversary.get_info():
            print('The simulation data of', self.adversary.get_attack_type_name() , 'is as follows', ':\n', self.adversary.get_info())
            stats.update(self.adversary.get_info())
            print('Double spending success times:', stats["double_spending_success_times"])
            
        # Network Property
        if not isinstance(self.network,network.LockstepNetwork):
            print('Block propagation times:', ave_block_propagation_times)
        if isinstance(self.network,network.TopologyNetwork) or isinstance(self.network,network.AdHocNetwork):
            print('Count of INV interactions:', inv_count)
            print('Count of full chain synchronization:', sync_full_chain_count)
            print('Count of data sending:', send_data_count)
        return stats

    def view_and_write(self, quantile):
        stats = self.view(quantile)
        # 原目录/文件入口；DAG全图输出沿用原有目录入口。
        if self.dag_enabled:
            from util.dag_output import write_results
            write_results(self, stats)
            return stats
        
        self.global_chain.printchain2txt()

        # save the results in the evaluation results.txt
        RESULT_PATH = global_var.get_result_path()
        with open(RESULT_PATH / 'evaluation results.txt', 'a+',  encoding='utf-8') as f:
            blocks_round = ['block_throughput_main', 'block_throughput_total']
            MB_round = ['throughput_main_MB', 'throughput_total_MB']
            rounds_block = ['average_block_time_main', 'average_block_time_total']

            for k,v in stats.items():
                if k in blocks_round:
                    print(f'{k}: {v} blocks/round', file=f)
                elif k in MB_round:
                    print(f'{k}: {v} MB/round', file=f)
                elif k in rounds_block:
                    print(f'{k}: {v} rounds/block', file=f)
                else:
                    print(f'{k}: {v}', file=f)

        if global_var.get_compact_outputfile():
            return stats

        # save local chain for all miners
        for miner in self.miners:
            miner.consensus.local_chain.printchain2txt(f"chain_data{str(miner.miner_id)}.txt")
        self.local_chain_tracker.dump_events(global_var.get_chain_data_path() / 'chain_dump.bin',
                                             [miner.get_local_chain() for miner in self.miners],
                                             self.honest_miner_ids)

        # show or save figures
        # self.global_chain.ShowStructure(self.miner_num)
        # block interval distribution
        self.miners[0].consensus.local_chain.GetBlockIntervalDistribution()

        other_list = self.adversary.get_eclipsed_ids()
        self.global_chain.ShowStructureWithGraphviz(other_list= other_list)
        if isinstance(self.network,network.TopologyNetwork):
            # 利用 isinstance 指定类型 方便调用类方法gen_routing_gragh_from_json()
            # self.network.save_rest_routing_process()
            self.network.gen_routing_gragh_from_json()
        return stats
    
    # 实际 SELF_GEN 消息进入原 NIC/network 的观察回调。
    def record_first_publication(self, block, round, target=None):
        """hash 唯一首次发布，不等于全网 wire/accepted 已完成。"""
        if isinstance(block, Block) and not block.isGenesis:
            if block.blockhash not in self.first_published_rounds:
                self.publication_cohorts[block.blockhash] = tuple(i for i in self.honest_miner_ids
                    if i != block.blockhead.miner)
            self.first_published_rounds.setdefault(block.blockhash, round)

    def _record_generated(self, block, round):
        """复制真实自产块到全知观察图，所有依赖来自已存在的生成记录。"""
        self.global_chain.add_dag_block(copy.deepcopy(block))
        # interval0补充池消费真实生成列表，不扫描公开共识图。
        if self.dag_dataitems_active and self.dataitem_params['dataitem_input_interval'] == 0 and block.blockhash not in self.generated_rounds:
            self.new_block_this_round.append(block)
        self.generated_rounds.setdefault(block.blockhash, round)

    def _exec_conflux(self, num_rounds, max_height, process_bar_type, disable_post_verification):
        """原矿工/网络自动演进；先全部挖矿、后传播，末轮批次完成接纳。

        drain 只执行 diffuse/process_received，实际挖矿轮数独立保存；严格 > 高度停止。
        全局观察图不可用于模板、缺父补齐或公开协议选择。
        """
        from util.dag_analysis import common_order_length

        if process_bar_type not in ('round', 'height'):
            raise ValueError('process_bar_type must be round or height')
        self.requested_rounds, self.requested_max_height = num_rounds, max_height
        t_0, cached_height = time.time(), 0
        in_mining = True  # 区分中断冻结与排空已冻结状态
        try:
            for current_round in range(1, num_rounds+1):
                self.total_round = current_round
                self.on_round_start(current_round)
                for miner in self.miners:
                    miner.round = current_round
                    miner.input_tape.append(('INSERT', self.input_dataitem))
                    messages = miner.BackboneProtocol(current_round)
                    if messages:
                        for block in messages:
                            if isinstance(block, Block):
                                self._record_generated(block, current_round)
                    miner.clear_tapes()
                self.network.diffuse(current_round)
                for miner in self.miners:
                    miner.process_received(current_round)
                height = self.miners[self.reference_miner_id].get_local_chain().get_height()
                if height > max_height:
                    self.stop_reason = 'height_limit'
                    break
                if process_bar_type == 'round':
                    self.process_bar(current_round, num_rounds, t_0, 'round/s')
                elif height > cached_height and max_height > 0:
                    cached_height = height
                    self.process_bar(height, max_height, t_0, 'block/s')
            self.ordered_at_mining_end = len(self.miners[self.reference_miner_id].get_local_chain().protocol_view.order)-1
            # 新增：只在挖矿结束边界统计LD，后续排空不能覆盖此吞吐分子。
            self.common_order_at_mining_end = common_order_length(
                miner.get_local_chain().protocol_view.order for miner in self.miners
            )
            self._freeze_conflux_confirmation()  # 
            self._freeze_conflux_dataitems()  # 数据项吞吐同样冻结
            in_mining = False  # 后续排空不能覆盖挖矿时分子
            # 可选固定收尾上限，不保证有损/延迟网络必定收敛；保留结束队列统计。
            for offset in range(1, self.dag_settings['drain_rounds']+1):
                round = self.total_round + offset
                self.local_chain_tracker.update_round(round)
                for miner in self.miners:
                    miner.round = round
                self.network.diffuse(round)
                for miner in self.miners:
                    miner.process_received(round, 'receive_drain')
                self.drain_rounds_executed = offset
        except KeyboardInterrupt:
            # 挖矿中断也保存当前有效排序；排空保留已冻结值。
            if in_mining:
                self.ordered_at_mining_end = len(self.miners[self.reference_miner_id].get_local_chain().protocol_view.order)-1
                # 新增：挖矿中断同样保存本次实际停止状态的共同前缀。
                self.common_order_at_mining_end = common_order_length(
                    miner.get_local_chain().protocol_view.order for miner in self.miners
                )
                self._freeze_conflux_confirmation()  # 
                self._freeze_conflux_dataitems()  # 中断保存当前数据项序
            self.stop_reason = 'interrupted'
            print('\nSimulation interrupted by user, saving existing results...')
        # 生成图也可安装观察投影，绝不作为矿工公开视图。
        from consensus.conflux_protocol import compute_view
        self.global_chain.set_protocol_view(compute_view(self.global_chain))
        if not disable_post_verification and not self.post_verification():
            raise AssertionError('Post verification failed: full Conflux DAG')
        height = self.miners[self.reference_miner_id].get_local_chain().get_height()
        # 工程时间不进入同seed语义指标。
        self.dag_execution_seconds = time.time()-t_0
        print(f'\nSimulation finished, reference Conflux pivot reach block height {height} '
              f'in {self.total_round} mining rounds; drain {self.drain_rounds_executed} rounds')

    def process_bar(self,process,total,t_0,unit='round/s'):
        bar_len = 50
        percent = (process)/total
        cplt = "■" * math.ceil(percent*bar_len)
        uncplt = "□" * (bar_len - math.ceil(percent*bar_len))
        time_len = time.time()-t_0+0.0000000001
        time_cost = time.gmtime(time_len)
        vel = process/(time_len)
        time_eval = time.gmtime(total/(vel+0.001))#Events: see events.log 
        print("\r{}{}  {:.5f}%  {}/{}  {:.2f} {}  {}:{}:{}>>{}:{}:{}  "\
        .format(cplt, uncplt, percent*100, process, total, vel, unit, time_cost.tm_hour, time_cost.tm_min, time_cost.tm_sec,\
            time_eval.tm_hour, time_eval.tm_min, time_eval.tm_sec),end="", flush=True)
        return vel

        
if __name__ == "__main__":
    import matplotlib.pyplot as plt

    times_prob = {0:0, 0.03: 0.944, 0.05: 1.804, 0.08: 2.767, 0.1: 3.308, 0.2: 5.421, 
              0.4: 8.797, 0.5: 10.396, 0.6: 12.071, 0.7: 13.956, 0.8: 16.217, 0.9: 19.377, 
              0.93: 20.777, 0.95: 21.971, 0.98: 24.683, 1.0: 29.027}
    times_vec ={0:0, 0.1: 1, 0.2: 2, 0.3:3, 0.4: 4, 0.5: 5.0, 0.6: 6.0, 0.7: 7.0, 0.8: 8.0, 0.9: 9.0, 1.0: 10} 


    times_tp = {0:0, 0.03: 9.297, 0.05: 12.257, 0.08: 15.889, 0.1: 17.185, 0.2: 21.459, 0.4: 26.592, 0.5: 28.493, 0.6: 30.343, 0.7: 32.232, 0.8: 34.231, 0.9: 36.993, 0.93: 38.194, 0.95: 39.256, 0.98: 41.946, 1.0: 46.706}
    times_outage = {0:0, 0.03: 9.627, 0.05: 12.988, 0.08: 16.45, 0.1: 17.94, 0.2: 22.193, 0.4: 27.405, 0.5: 29.353, 0.6: 31.262, 0.7: 33.265, 0.8: 35.496, 0.9: 38.663, 0.93: 40.298, 0.95: 41.889, 0.98: 45.681, 1.0: 52.662}
    times_wo_parition = {0:0,  0.05: 21.713, 0.08: 27.103, 0.1: 29.326, 0.2: 42.763, 0.4: 61.751, 0.5: 68.969, 0.6: 80.483, 0.8: 92.329, 0.9: 103.141, 0.93: 107.383, 0.95: 110.757, 0.98: 119.123, 1.0: 145.109}
    times_dynamic ={0:0, 0.03: 18.237, 0.05: 24.153, 0.08: 30.627, 0.1: 34.757, 0.2: 56.545, 0.4: 81.044, 0.5: 84.115, 0.6: 103.68, 0.7: 128.212, 0.8: 147.1, 0.9: 196.187, 0.93: 238.383, 0.95: 360.152, 0.98: 2226.653, 1.0: 3848.5}
    rcv_rates = list(times_tp.keys())
    t = list(times_tp.values())
    t = [tt/t[-1] for tt in t]
    plt.plot(t,rcv_rates,"--o", label= "Topology Network")

    rcv_rates = list(times_outage.keys())
    t = list(times_outage.values())
    t = [tt/t[-1] for tt in t]
    plt.plot(t,rcv_rates,"--o", label= "Topology Network + Link Outage")

    rcv_rates = list(times_wo_parition.keys())
    t = list(times_wo_parition.values())
    t = [tt/t[-1] for tt in t]
    plt.plot(t,rcv_rates,"--o", label= "Topology Network + Dynamic")

    rcv_rates = list(times_dynamic.keys())
    t = list(times_dynamic.values())
    t = [tt/t[-1] for tt in t]
    plt.plot(t,rcv_rates,"--o", label= "Topology Network + Dynamic + 3Patitions")

    rcv_rates = list(times_prob.keys())
    t = list(times_prob.values())
    t = [tt/t[-1] for tt in t]
    plt.plot(t,rcv_rates,"--^", label= "Stochastic Propagation Network")
    rcv_rates = list(times_vec.keys())
    t = list(times_vec.values())
    t = [tt/t[-1] for tt in t]
    plt.plot(t,rcv_rates,"--*", label= "Deterministic Propagation Network")
    plt.xlabel("Normalized number of rounds passed")
    plt.ylabel("Ratio of received miners")
    plt.legend()
    plt.grid()
    plt.show()
