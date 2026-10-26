import math
import logging
from abc import ABCMeta, abstractmethod
from collections import defaultdict

import global_var
from data import Block, Message
from functions import INT_LEN

# packet type
DIRECT = "direct"
GLOBAL = "global"

# link errors
ERR_OUTAGE = "err_outage"

logger = logging.getLogger(__name__)

class DataSegment(object):
    def __init__(self, origin_msg:Block, seg_id:int):
        self.origin_block = origin_msg
        self.seg_id = seg_id
    
    def __repr__(self) -> str:
        return str((self.origin_block.name, self.seg_id))

class Packet(object):
    def __init__(self, source:int, payload:Message|list[DataSegment]):
        self.source = source
        self.payload = payload
        
class INVMsg(object):
    def __init__(self, source:int, target:int, block:Block,isFullChain:bool=False, *, frontier_hashes:tuple[bytes,...]=()):
        self.source = source
        self.target = target
        self.block_or_seg:Block = block
        self.isFullChain = isFullChain
        self.frontier_hashes = tuple(frontier_hashes)  # 免费库存只有hash

class GetDataMsg(object):
    def __init__(self, source:int=None, target:int=None, 
                 req_blocks:list[Block]=None, require:bool=None):
        self.source = source
        self.target:int = target
        self.req_blocks:list[Block] = req_blocks
        self.req_segs:list = []
        self.isRequired:bool = require
        # DAG反馈只给hash/缺片；旧req_blocks/req_segs保留。
        self.known_hashes = frozenset()
        self.missing_segments = {}


class Network(metaclass=ABCMeta):
    """网络抽象基类"""

    def __init__(self) -> None:
        self.MINER_NUM = global_var.get_miner_num()  # 网络中的矿工总数，为常量
        self.NET_RESULT_PATH = global_var.get_net_result_path()
        self.withTopology = False
        self.withSegments = False
        self._dataitem_param = dict()
        # 冻结大小配置；统计只读观察，不向共识补图。
        self._dag_settings = global_var.get_dag_settings()
        self._dag_wire_receivers = {}
        self._dag_published = {}
        self._dag_thresholds = {}

    def message_preprocessing(self, msg:Message):
        # 幂等几何计算；wire元数据不能改变计费/收片数量。
        if isinstance(msg, Block) and msg.blockhead.header_version == 2:
            from consensus.conflux_codec import encode_header, validate_block_header
            if not validate_block_header(msg):
                return False
            # 数据项开启时沿用原名义MB/项，容量0视为关闭。
            if self._dataitem_param.get('dataitem_enable'):
                count, partial = divmod(len(msg.blockhead.content), 8)
                if partial or count > self._dataitem_param['max_block_capacity']:
                    return False
                body = count * self._dataitem_param['dataitem_size']
            else:
                body = global_var.get_blocksize()
            overhead = (len(encode_header(msg.blockhead))-len(msg.blockhead.content))/1_000_000
            msg.size = body + (overhead if self._dag_settings.get('size_model') == 'body_plus_header' else 0)
            segment_size = global_var.get_segmentsize()
            msg.segment_num = max(1, math.ceil(msg.size/segment_size)) if segment_size > 0 else 1
            return True
        if self._dataitem_param.get('dataitem_enable') and isinstance(msg, Block):
            msg.size = self._dataitem_param['dataitem_size'] * len(msg.blockhead.content) / 2 / INT_LEN
            if msg.size > self._dataitem_param['max_block_capacity'] * self._dataitem_param['dataitem_size']:
                logger.warning("The data items in block content exceeds the maximum capacity!")
                return False
            if global_var.get_segmentsize() > 0 and msg.size > 0:
                msg.segment_num = math.ceil(msg.size/global_var.get_segmentsize())
        return True

    def note_dag_send(self, block, source, round):
        """首次真实链路发送为阈值延迟起点。"""
        h = block.blockhash
        self._dag_published.setdefault(h, round)
        self._dag_wire_receivers.setdefault(h, set()).update((block.blockhead.miner, source))
        # 含生产者的低比例阈值可能在发布时间已经满足。
        self.note_dag_receive(block, source, round)

    def note_dag_receive(self, block, target, round):
        """完整块按hash/目标去重，混合分片分别记阈值。"""
        h = block.blockhash
        if h not in self._dag_published:
            return  # 外部fixture直接注入未经过网络，不伪造发布统计。
        received = self._dag_wire_receivers[h]
        received.add(target)
        rates = getattr(self, '_stat_prop_times', {})
        crossed = self._dag_thresholds.setdefault(h, set())
        for i, rate in enumerate(rates):
            if rate not in crossed and len(received) >= math.ceil(rate*self.MINER_NUM):
                crossed.add(rate)
                rates[rate] += round-self._dag_published[h]
                self._block_num_bpt[i] += 1

    @abstractmethod
    def set_net_param(self, *args, **kargs):
        pass

    @abstractmethod
    def access_network(self, new_msgs:list[Message|tuple[Message, int]], minerid:int, round:int, 
                       target:int = None, sendTogether:bool = False):
        pass

    @abstractmethod
    def diffuse(self, round):
        pass