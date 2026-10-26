import time
from typing import List, Tuple

import global_var
from external import common_prefix
from functions import BYTE_ORDER, INT_LEN, HASH_LEN, hash_bytes

from .consensus_abc import Consensus


class PoW(Consensus):

    class BlockHead(Consensus.BlockHead):
        '''适用于PoW共识协议的区块头'''
        __slots__ = ['target', 'nonce']
        def __init__(self, preblock: Consensus.Block = None, timestamp=0, content=b'', miner_id=-1,
                     target = (2**(8*HASH_LEN) - 1).to_bytes(HASH_LEN, BYTE_ORDER), nonce = 0,
                     *, header_version=1, refhashes=(), prehash=None):  # 
            # 同一原生头类型承载可选v2字段，默认仍为v1。
            super().__init__(preblock, timestamp, content, miner_id,
                             header_version=header_version, refhashes=refhashes, prehash=prehash)
            self.target = target  # 难度目标
            self.nonce = nonce  # 随机数

        def __eq__(self, value):
            return super().__eq__(value) and self.target == value.target and self.nonce == value.nonce

        def calculate_blockhash(self) -> bytes:
            # 仅v2使用完整承诺；以下v1字节公式完全保留。
            if isinstance(self.header_version, bool) or not isinstance(self.header_version, int) or self.header_version not in (1, 2):
                raise ValueError('header_version must be 1 or 2')
            if self.header_version == 2:
                from .conflux_codec import calculate_header_hash
                return calculate_header_hash(self)
            # 被修改的v1头也不能带未承诺引用。
            if self.refhashes:
                raise ValueError('refhashes must be empty for header_version=1')
            data = self.miner.to_bytes(INT_LEN, BYTE_ORDER, signed=True)+ \
                    self.content+ self.prehash + \
                    self.nonce.to_bytes(INT_LEN, BYTE_ORDER)
            return hash_bytes(data).digest()

    def __init__(self,miner_id,consensus_params:dict):
        super().__init__(miner_id=miner_id)
        self.INT_SIZE = INT_LEN
        self.BYTEORDER = BYTE_ORDER
        self.miner_id_bytes = miner_id.to_bytes(self.INT_SIZE, self.BYTEORDER, signed=True)
        self.ctr=0 #计数器
        self.target = bytes.fromhex(consensus_params['target'])
        if consensus_params['q_distr'] == 'equal':
            self.q = consensus_params['q_ave']
        else:
            q_distr = consensus_params['q_distr']
            if isinstance(q_distr,list):
                self.q = q_distr[miner_id]
            else:
                raise ValueError("q_distr should be a list or the string 'equal'")

    def setparam(self,**consensus_params):
        '''
        设置pow参数,主要是target
        '''
        self.target = bytes.fromhex(consensus_params.get('target') or self.target)
        self.q = consensus_params.get('q') or self.q

    def mining_consensus(self, miner_id:bytes, isadversary, x, round):
        '''计算PoW\n
        param:
            Miner_ID 该矿工的ID type:int
            x 写入区块的内容 type:any
            qmax 最大hash计算次数 type:int
        return:
            newblock 挖出的新块 type:None(未挖出)/Block
            pow_success POW成功标识 type:Bool
        '''
        pow_success = False
        if self.q <= 0:
            return (None, pow_success)
        b_last = self.local_chain.last_block # 链中最后一个块
        prehash = b_last.blockhash

        intermediate_hasher = hash_bytes(miner_id + x + prehash)
        
        i = 0
        while i < self.q:
            self.ctr = self.ctr+1
            hasher = intermediate_hasher.copy()
            hasher.update(self.ctr.to_bytes(INT_LEN, BYTE_ORDER))
            currenthash=hasher.digest()#计算哈希
            if currenthash<self.target:
                pow_success = True
                # blockhead = PoW.BlockHead(b_last,time.time_ns(),x,miner_id,self.target,self.ctr)
                # Use round instead as real world timestamp is meaningless in chainxim
                blockhead = self.BlockHead(b_last,round,x,int.from_bytes(miner_id, BYTE_ORDER, signed=True),
                                           self.target,self.ctr)
                blocknew = self.Block(blockhead,b_last,isadversary,global_var.get_blocksize())
                self.ctr = 0
                return (blocknew, pow_success)
            else:
                i = i+1
        return (None, pow_success)
        
    def local_state_update(self, round):
        # algorithm 2 比较自己的chain和收到的chain并相应更新本地链
        # output:
        #   lastblock 最长链的最新一个区块
        new_update = False  # 有没有更新
        chain_update = []
        tree_update = []
        original_last_block = None
        for incoming_block in self.receive_tape:
            if not isinstance(incoming_block, self.Block):
                continue
            if self.valid_block(incoming_block):
                prehash = incoming_block.blockhead.prehash
                if insert_point := self.local_chain.search_block_by_hash(prehash):
                    conj_block = self.local_chain.add_blocks(blocks=[incoming_block], insert_point=insert_point,
                                                             deepcopy=False)
                    fork_tip, touched_blocks = self.synthesize_fork(conj_block)
                    tree_update.extend(touched_blocks)
                    depthself = self.local_chain.get_height()
                    depth_incoming_block = fork_tip.get_height()
                    if depthself < depth_incoming_block:
                        original_last_block = self.local_chain.last_block if original_last_block is None else original_last_block
                        self.local_chain.set_last_block(fork_tip)
                        new_update = True
                else:
                    self.block_buffer.setdefault(prehash, [])
                    self.block_buffer[prehash].append(incoming_block)

        if new_update:
            blocktmp = self.local_chain.get_last_block()
            fork_point = common_prefix(blocktmp, original_last_block)
            while blocktmp.blockhash != fork_point.blockhash:
                chain_update.insert(0, blocktmp)
                blocktmp = blocktmp.parentblock
        return self.local_chain, chain_update, tree_update

    def valid_block(self,block:Consensus.Block):
        '''
        验证单个区块是否PoW合法\n
        param:
            block 要验证的区块 type:Block
        return:
            block_vali 合法标识 type:bool
        '''
        # 旧PoW接收路径仅接受v1，防止跨协议消息混入旧单链。
        # True/1.0与1数值相等，须在计算hash前严格拒绝。
        version = block.blockhead.header_version
        if isinstance(version, bool) or not isinstance(version, int) or version != 1 or block.blockhead.refhashes:
            return False
        btemp = block
        target = btemp.blockhead.target
        hash = btemp.calculate_blockhash()
        if hash >= target:
            return False
        else:
            return True
