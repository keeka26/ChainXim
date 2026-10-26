import copy
from itertools import chain
from abc import ABCMeta, abstractmethod

from functions import BYTE_ORDER, INT_LEN, hash_bytes

from .message import Message


# 旧pickle缺新增slots时恢复默认值；仅处理本地可信快照。
def _restore_slots_state(obj, state):
    """兼容默认pickle的dict/slot状态，不重新计算声明hash或规范化输入。"""
    dict_state, slot_state = state if isinstance(state, tuple) else (state, {})
    for values in (dict_state, slot_state):
        for key, value in (values or {}).items():
            setattr(obj, key, value)


class BlockHead(metaclass=ABCMeta):
    # 空refs/版本1保持旧模式，版本2用于Conflux承诺。
    __slots__ = ['prehash', 'timestamp', 'content', 'miner', 'header_version', 'refhashes']
    __omit_keys = {} # The items to omit when printing the object
    
    # 新协议量keyword-only，不改变原四个位置参数。
    def __init__(self, prehash=b'', timestamp=0, content = b'', Miner = -1,
                 *, header_version=1, refhashes=()):
        self.prehash:bytes = prehash  # 前一个区块的hash
        self.timestamp:int = timestamp  # 时间戳
        self.content:bytes = content
        self.miner:int = Miner  # 矿工
        # 仅发送端构造规范化；接收检查采用纯codec。
        if isinstance(header_version, bool) or not isinstance(header_version, int) or header_version not in (1, 2):
            raise ValueError('header_version must be 1 or 2')
        self.header_version:int = header_version
        if header_version == 2:
            from consensus.conflux_codec import normalize_refhashes
            self.refhashes:tuple[bytes, ...] = normalize_refhashes(prehash, refhashes)
        else:
            if not isinstance(refhashes, (tuple, list)) or refhashes:
                raise ValueError('refhashes must be empty for header_version=1')
            self.refhashes = ()

    # 旧快照不存在新增字段，默认恢复旧头身份。
    def __setstate__(self, state):
        """恢复头状态并补v1默认量，保持旧hash/打印和派生头额外字段。"""
        _restore_slots_state(self, state)
        if not hasattr(self, 'header_version'):
            self.header_version = 1
        if not hasattr(self, 'refhashes'):
            self.refhashes = ()

    def __eq__(self, value):
        if not isinstance(value, BlockHead):
            return False
        return self.prehash == value.prehash and \
               self.timestamp == value.timestamp and \
               self.content == value.content and \
               self.miner == value.miner and \
               self.header_version == value.header_version and \
               self.refhashes == value.refhashes  # 协议身份
    
    @abstractmethod
    def calculate_blockhash(self) -> bytes:
        '''
        计算区块的hash
        return:
            hash type:bytes
        '''
        data = self.miner.to_bytes(INT_LEN, BYTE_ORDER,signed=True) + \
                self.content.to_bytes(INT_LEN, BYTE_ORDER) + \
                self.prehash
        return hash_bytes(data).digest()

    def __repr__(self) -> str:
        bhlist = []
        keys = chain.from_iterable(list(getattr(s, '__slots__', []) for s in self.__class__.__mro__) + list(getattr(self, '__dict__', {}).keys()))
        for k in keys:
            # 旧repr逐字保留；只有v2显示新协议字段。
            if self.header_version == 1 and k in ('header_version', 'refhashes'):
                continue
            if k not in self.__omit_keys:
                v = getattr(self, k)
                # 引用父用hex展示，保留主父与引用父角色。
                rendered = '[' + ', '.join(h.hex() for h in v) + ']' if k == 'refhashes' else (str(v) if not isinstance(v, bytes) else v.hex())
                bhlist.append(k + ': ' + rendered)
        return '\n'.join(bhlist)


class Block(Message):
    # rank是本地元数据，不进入头hash。
    __slots__ = ['__blockhead', 'height', 'blockhash', 'isAdversaryBlock', 'next', 'parentblock', 'isGenesis', 'dag_rank']
    __omit_keys = {'segment_num'} # The items to omit when printing the object

    def __init__(self, name, blockhead: BlockHead, height = None, 
                 isadversary=False, isgenesis=False, blocksize_MB=2, *, dag_rank=None):  # 
        super().__init__(name, blocksize_MB)
        self.__blockhead = blockhead
        self.height = height
        self.blockhash = blockhead.calculate_blockhash()
        self.isAdversaryBlock = isadversary
        self.next:list[Block] = []  # 子块列表
        self.parentblock:Block = None  # 母块
        self.isGenesis = isgenesis
        # 非创世/缺父时rank可为None，依赖闭合后计算。
        if dag_rank is not None and (isinstance(dag_rank, bool) or not isinstance(dag_rank, int) or dag_rank < 0):
            raise ValueError('dag_rank must be a nonnegative integer or None')
        self.dag_rank = 0 if blockhead.header_version == 2 and isgenesis and dag_rank is None else dag_rank
        # super().__init__(int(random.uniform(0.5, 2)))
        # 单位:MB 随机 0.5~1 MB
        
    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result

        slots = chain.from_iterable((getattr(s, '__slots__', [])) for s in self.__class__.__mro__)
        for k in slots:
            # 所有原生派生块的传播副本都断开邻接。
            if k == 'next':
                setattr(result, k, [])
                continue
            if k == 'parentblock':
                setattr(result, k, None)
                continue
            if k == '__name':
                key = '_Message__name'
            elif k == '__blockhead':
                key = '_Block__blockhead'
            else:
                key = k
            setattr(result, key, copy.deepcopy(getattr(self, key), memo))

        if var_dict := getattr(self, '__dict__', None):
            for k, v in var_dict.items():
                setattr(result, k, copy.deepcopy(v, memo))
        return result

    # 恢复修改前的旧块快照，不重算/覆盖声明ID。
    def __setstate__(self, state):
        """补本地rank默认值；其他原字段和邻接按原pickle状态保留。"""
        _restore_slots_state(self, state)
        if not hasattr(self, 'dag_rank'):
            self.dag_rank = None

    def __repr__(self) -> str:
        def _formatter(d, mplus=1):
            m = max(map(len, list(d.keys()))) + mplus
            s = '\n'.join([k.rjust(m) + ': ' + 
                           _indenter(str(v) if not isinstance(v, bytes) 
                           else v.hex(), m+2) for k, v in d.items()])
            return s
        def _indenter(s, n=0):
            split = s.split("\n")
            indent = " "*n
            return ("\n" + indent).join(split)
        
        slots = chain.from_iterable(getattr(s, '__slots__', []) for s in self.__class__.__mro__)
        var_dict = {}
        for k in slots:
            # 旧块打印不显示新增rank，保持原txt布局。
            if k == 'dag_rank' and self.blockhead.header_version == 1:
                continue
            if k == '__name':
                key = '_Message__name'
            elif k == '__blockhead':
                key = '_Block__blockhead'
            else:
                key = k
            var_dict[k] = getattr(self, key)

        if hasattr(self, '__dict__'):
            var_dict.update(self.__dict__)
        var_dict.update({'next': [b.name for b in self.next if self.next], 
                      'parentblock': self.parentblock.name if self.parentblock is not None else None})
        for omk in self.__omit_keys:
            if omk in var_dict:
                del var_dict[omk]
        return '\n'+ _formatter(var_dict)

    def __eq__(self, value):
        if not isinstance(value, Block):
            return False
        return self.blockhead == value.blockhead

    @property
    def blockhead(self):
        return self.__blockhead

    def calculate_blockhash(self):
        self.blockhash = self.blockhead.calculate_blockhash()
        return self.blockhash

    def get_height(self):
        return self.height
    
class BlockCarrier(metaclass=ABCMeta):
    @abstractmethod
    def get_block(self) -> Block:
        ...