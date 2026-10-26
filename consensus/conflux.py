"""原生PoW接口中的Conflux v2挖矿与接纳。

继承原PoW的Block/BlockHead、q/ctr和方法形状，使用规范SHA-256编码与多父依赖resolver。
接收批末/自产后计算并安装pivot/epoch/order；默认模板读该尾。
原入口已接通；旧 PoW 算法保持。
"""

import copy
import logging
import math
import random
import time  # 工程计时，不参与协议决策。

import data
import global_var

from .pow import PoW
from .conflux_codec import calculate_header_hash, encode_header, validate_block_header
from .dependency import DependencyResolver

from .conflux_confirmation import ConfirmationTracker

logger = logging.getLogger(__name__)


def _budget(value):
    """保留int/float和合法0；拒绝bool、字符串、负数及非有限浮点。"""
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or value < 0
        or isinstance(value, float)
        and not math.isfinite(value)
    ):
        raise ValueError("hash budget must be a finite nonnegative number")
    return value


def _target(value):
    """参数层64hex转为固定32字节目标；允许全零，不吞掉显式值。"""
    if not isinstance(value, str) or len(value) != 64:
        raise ValueError("target must be a 64-character hexadecimal string")
    try:
        target = bytes.fromhex(value)
    except ValueError as error:
        raise ValueError("target must be a 64-character hexadecimal string") from error
    if len(target) != 32:
        raise ValueError("target must encode exactly 32 bytes")
    return target


class ConfluxPoW(PoW):
    """一份原生accepted图，resolver只持有未闭合依赖。

    普通块只允许固定配置target、合法网络矿工ID和hash<target。固定创世例外。
    校验不改变声明hash，也不要求远端采用接收方当前主父/tips。
    """

    def __init__(self, miner_id, consensus_params: dict):
        """验证typed参数后复用PoW初始化；虚方法创建唯一固定v2创世。"""
        miner_num = global_var.get_miner_num()
        if type(miner_num) is not int or not 0 < miner_num <= 2**31:
            raise ValueError(
                "miner_num must be a positive integer within the v2 ID range"
            )
        if type(miner_id) is not int or not 0 <= miner_id < miner_num:
            raise ValueError("miner_id must be an integer inside this network")
        if not isinstance(consensus_params, dict):
            raise ValueError("consensus_params must be a typed dictionary")
        _target(consensus_params.get("target"))
        _budget(consensus_params.get("q_ave"))
        distribution = consensus_params.get("q_distr")
        if distribution != "equal":
            if not isinstance(distribution, list) or len(distribution) != miner_num:
                raise ValueError(
                    "q_distr must be equal or a list with one budget per miner"
                )
            for value in distribution:
                _budget(value)
        # 原参数通道保留实例配置；共识不读取全局生成图。
        self.dag_settings = dict(consensus_params.get("dag_settings", {}))
        # 本次run的容量/编码规则；不查询全局输入合法集合。
        self.dataitem_settings = dict(consensus_params.get("dataitem_settings", {}))
        # 确认状态属于本地共识；默认none，原N默认为6。
        self.confirmation_tracker = ConfirmationTracker(
            self.dag_settings.get("confirmation_policy", "none"),
            consensus_params.get("N", 6),
        )
        self.miner_num = miner_num  # 冻结校验范围，不随其他实验改变全局值。
        # 只读输入审计；compact不保留大日志。
        self.audit_enabled = not global_var.get_compact_outputfile()
        (
            self.audit_events,
            self._audit_batch,
            self._audit_rejections,
            self._audit_dataitem_inputs,
        ) = ([], [], [], [])
        self._audit_accepted, self._audit_pending, self._audit_pivot = set(), set(), ()
        self._audit_origin = "received"
        self.audit_invalid_inputs = 0
        self.audit_pending_first = {}
        self.audit_protocol_seconds = self.audit_ingest_seconds = 0.0
        super().__init__(miner_id, consensus_params)
        self.dependency_resolver = DependencyResolver(
            self.local_chain, self.valid_block
        )
        # 原block_buffer仍存在但不使用：它只能表达单主父缓存。
        self._deferred_forward: list[data.Block] = []
        self._update_protocol_view()  # 初始创世view

    def create_genesis_block(
        self, chain: data.Chain, blockheadextra: dict = None, blockextra: dict = None
    ):
        """初始化采用固定v2创世，不能通过extra改变其协议身份。"""
        if blockheadextra or blockextra:
            raise ValueError(
                "Conflux genesis fields are fixed and cannot be overridden"
            )
        head = self.BlockHead(header_version=2)
        chain.add_dag_block(self.Block(head))

    def setparam(self, **consensus_params):
        """原接口保留；target/q先整体核查，再原子更新，合法0有效。

        target仅在只有创世且没有接收/缺父消息时可改变，防止一份固定难度图混目标。
        后续难度调整是独立协议能力；q调整不重置原持续nonce计数器。
        """
        if set(consensus_params) - {"target", "q"}:
            raise ValueError("Conflux setparam supports only target and q")
        target = (
            _target(consensus_params["target"])
            if "target" in consensus_params
            else self.target
        )
        q = _budget(consensus_params["q"]) if "q" in consensus_params else self.q
        if target != self.target and (
            len(self.local_chain.block_set) > 1
            or self.receive_tape
            or self.dependency_resolver.pending
        ):
            raise ValueError(
                "cannot change fixed target after ordinary blocks or queued messages"
            )
        self.target, self.q = target, q

    def _local_parent(self, block: data.Block):
        """显式父须身份有效且本地accepted；只使用canonical本地对象。"""
        if not self.valid_block(block):
            raise ValueError("parent must have a valid Conflux identity and PoW")
        local = self.local_chain.block_set.get(block.blockhash)
        if local is None or not self.valid_block(local):
            raise ValueError("all template parents must be locally accepted")
        return local

    def build_mining_template(
        self,
        x: bytes,
        round: int,
        *,
        main_parent: data.Block = None,
        refhashes: tuple[bytes, ...] = None
    ) -> PoW.BlockHead:
        """固定本地模板；默认主父为已安装链尾，引用全部有效accepted tips。

        正常生命周期已将论文pivot尾安装到last_block。
        显式参数允许本地历史父/引用，
        不加入任意父数上限。头只含直接依赖，缺父pending不参与模板。
        """
        main = self._local_parent(
            self.local_chain.last_block if main_parent is None else main_parent
        )
        refs = (
            tuple(
                block.blockhash
                for block in self.local_chain.get_tips()
                if block.blockhash != main.blockhash
            )
            if refhashes is None
            else refhashes
        )
        if not isinstance(refs, tuple):
            raise ValueError("refhashes must be a tuple of accepted hashes")
        head = self.BlockHead(
            main,
            round,
            x,
            self.miner_id,
            self.target,
            0,
            header_version=2,
            refhashes=refs,
        )
        encode_header(head)  # 核查内容/逻辑round/引用规范，不执行nonce尝试。
        for reference in head.refhashes:
            block = self.local_chain.block_set.get(reference)
            if block is None or not self.valid_block(block):
                raise ValueError(
                    "all reference parents must be locally accepted and valid"
                )
        return head

    def mining_consensus(self, miner_id: bytes, isadversary, x: bytes, round: int):
        """原(None/Block,bool)形状；一次固定模板最多ceil(q)次真实SHA尝试。

        q=0立即返回，不构造模板/试nonce。失败保留ctr；首次成功停止并重置ctr。
        ctr对应u32 nonce，最大值可尝试一次，下一次明确OverflowError，绝不回绕。
        本函数只产生候选；consensus_process统一经resolver接纳。
        """
        if self.q <= 0:
            return None, False
        if not isinstance(miner_id, bytes) or len(miner_id) != self.INT_SIZE:
            raise ValueError("miner_id must be the original signed four-byte ID")
        miner = int.from_bytes(miner_id, self.BYTEORDER, signed=True)
        if not 0 <= miner < self.miner_num:
            raise ValueError("mining ID must be inside this network")

        template = self.build_mining_template(x, round)
        template.miner = miner  # 保留原mining_consensus的显式矿工ID参数。
        attempts = 0
        while attempts < self.q:
            if type(self.ctr) is not int or not 0 <= self.ctr < 2**32:
                raise ValueError("ctr must be a valid u32 counter")
            if self.ctr == 2**32 - 1:
                raise OverflowError("Conflux nonce exhausted; counter cannot wrap")
            self.ctr += 1
            template.nonce = self.ctr
            currenthash = calculate_header_hash(template)
            attempts += 1
            if currenthash < self.target:
                main = self.local_chain.block_set[template.prehash]
                block = self.Block(
                    template, main, isadversary, global_var.get_blocksize()
                )
                self.ctr = 0
                return block, True
        return None, False

    def valid_block(self, block: data.Block) -> bool:
        """纯验证规范声明身份、固定target、网络矿工与hash<target。

        创世按固定规范，不受实例普通块target约束。父存在与深度由resolver/Chain负责；
        不要求远端主父/tips与本地当前视图相同，不调用会覆盖声明ID的旧Block方法。
        """
        if not isinstance(block, data.Block) or not validate_block_header(block):
            return False
        if block.isGenesis:
            return True
        head = block.blockhead
        # active模式逐块验证完整8字节编码和名义容量。
        if self.dataitem_settings.get("active") and (
            len(head.content) % 8
            or len(head.content) // 8 > self.dataitem_settings["max_block_capacity"]
        ):
            return False
        return (
            head.target == self.target
            and 0 <= head.miner < self.miner_num
            and block.blockhash < self.target
        )

    def has_received(self, msg: data.Message):
        """先验证身份/PoW再查accepted/pending/tape；伪声明不占用合法ID。"""
        if not self.valid_block(msg):
            return False
        h = msg.blockhash
        if h in self.local_chain.block_set or h in self.dependency_resolver.pending:
            return True
        return any(
            self.valid_block(queued) and queued.blockhash == h
            for queued in self.receive_tape
        )

    def receive_block(self, rcvblock: PoW.Block):
        """保留bool接收返回；先验证/去重，再复制到原receive_tape，下一轮入图。"""
        # 拒绝是输入次数，不永久缓存伪声明hash。
        if not self.valid_block(rcvblock):
            self.audit_invalid_inputs += 1
            if self.audit_enabled:
                from util.dag_audit import block_payload

                self._audit_rejections.append(block_payload(rcvblock))
            return False
        if self.has_received(rcvblock):
            return False
        self.receive_tape.append(copy.deepcopy(rcvblock))
        random.shuffle(self.receive_tape)  # 保留原批次接收习惯；统一计算批末view。
        return True

    # 协议状态仍存原Chain，避免两份order/delta。
    @property
    def last_protocol_delta(self):
        """向Miner提供最近一次只读排序差量，用于撤销和重放。"""
        return self.local_chain.last_protocol_delta

    # 全序与确认序是两个接口，旧全序不改。
    def get_confirmed_order(self) -> tuple[bytes, ...]:
        """当前本地经验确认前缀；none返回空tuple。"""
        return self.confirmation_tracker.current_order

    @property
    def last_confirmation_delta(self):
        """最近协议边界的可撤销确认差量。"""
        return self.confirmation_tracker.last_delta

    def _update_protocol_view(self, round=0, phase="initial") -> list[data.Block]:
        """刷新本地协议投影；返回canonical added后缀，不等同首次接纳。"""
        # 安装后记边界；只计时，不改算法/随机调用。
        started = time.perf_counter()
        # DAG性能优化：图变更时基于已验证索引计算一次；空批次仍保留确认/审计边界。
        delta = self.local_chain._refresh_protocol_view()
        # 只在完整批次安装view后重算，排空接收也有效。
        # DAG增量优化：本次规范排序差量为确认更新提供精确的共同前缀证明。
        self.confirmation_tracker.update(
            self.local_chain.protocol_view, round, phase, protocol_delta=delta
        )
        self.audit_protocol_seconds += time.perf_counter() - started
        from util.dag_audit import record_boundary

        record_boundary(self, round, phase)
        return [self.local_chain.block_set[h] for h in delta.added]

    def local_state_update(self, round: int):
        """返回(原Chain,order更新list,首次有效接纳/转发list)。

        全批resolver级联后统一安装view，第二项为order新增/重放后缀。
        tape清理仍由原Miner.clear_tapes负责。自产解锁的远端块下一次更新按接收来源转发。
        """
        # 记录实际tape，不能以accepted标记冒充输入。
        if self.audit_enabled:
            from util.dag_audit import block_payload

            self._audit_batch = [block_payload(b) for b in self.receive_tape]
            self._audit_origin = "received"
        self.audit_invalid_inputs += sum(
            not self.valid_block(b) for b in self.receive_tape
        )
        started = time.perf_counter()
        activated = list(self._deferred_forward)
        self._deferred_forward = []
        for incoming in self.receive_tape:
            activated.extend(self.dependency_resolver.submit(incoming))
        self.audit_ingest_seconds += time.perf_counter() - started
        ordered = self._update_protocol_view(round, "receive_batch")  # 批末一次
        return self.local_chain, ordered, activated

    def consensus_process(self, isadversary, x: bytes, round: int):
        """自产也经同一resolver接纳；保留原([自产Block],True)/(None,False)。

        自产闭合远端后继时已一起入图，但放入延迟转发列表；不能把远端块标为SELF_GEN。
        自产及同次依赖级联后安装view，再由Miner更新本地状态。
        """
        block, success = self.mining_consensus(
            self.miner_id_bytes, isadversary, x, round
        )
        if not success:
            return None, False
        # 自产候选仍重验PoW/依赖，不重新挖矿重放。
        if self.audit_enabled:
            from util.dag_audit import block_payload

            self._audit_batch = [block_payload(block)]
            self._audit_origin = "generated"
        activated = self.dependency_resolver.submit(block)
        if not activated:
            return None, False
        local = self.local_chain.block_set[block.blockhash]
        self._deferred_forward.extend(
            candidate
            for candidate in activated
            if candidate.blockhash != block.blockhash
        )
        self._update_protocol_view(
            round, "self_generated"
        )  # 自产边界，不重复返回远端SELF_GEN
        logger.info("round %d, M%d mined %s", round, self.miner_id, local.name)
        return [local], True

    def audit_item_input(self, content):
        """记录真实select/INSERT内容，重放可恢复本地池。"""
        if self.audit_enabled:
            self._audit_dataitem_inputs.append(content.hex())

    def valid_dag(self, chain: data.Chain) -> bool:
        """检查完整accepted图结构/缓存/深度与所有块PoW，不只沿所选主父链。"""
        if not isinstance(chain, data.Chain) or not chain._dag_mode:
            return False
        try:
            chain._inspect_dag()
            return all(self.valid_block(block) for block in chain.block_set.values())
        except (ValueError, TypeError, AttributeError, KeyError, OverflowError):
            return False

    def valid_chain(self, lastblock: PoW.Block):
        """保留旧bool接口但核查完整本地图，拒绝以单主父遍历冒充DAG验证。"""
        return (
            self.valid_block(lastblock)
            and lastblock.blockhash in self.local_chain.block_set
            and self.valid_dag(self.local_chain)
        )
