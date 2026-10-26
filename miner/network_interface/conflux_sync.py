"""原 NIC 的诚实 DAG 同步/分片助手。

只保存传输状态；唯一 accepted 图仍是 miner.consensus.local_chain。
库存INV/GETDATA只携带hash/片ID；逐消息INV沿用原对象表示但不入tape/图。
所有正文的有效接收仍经过原链路计费；免费控制大小/校验成本未建模。
"""

from copy import deepcopy
from data import Block
from network import DataSegment, INVMsg, GetDataMsg, ERR_OUTAGE
from .._consts import _IDLE, SELF_GEN_MSG, OUTER_RCV_MSG


class ConfluxSync:
    """使用原 output queues/channel states，不另建矿工、网络或图存储。"""

    def __init__(self, nic):
        self.nic = nic
        self.miner = nic.miner
        settings = self.miner.consensus.dag_settings
        self.interval = settings.get("sync_interval", 5)
        self.batch_size = settings.get("sync_batch_size", 128)
        self.parts = {}  # hash -> {block: 脱离邻接的wire副本, missing: 分片ID集合}
        self.queued = {}  # peer -> 已排队或正在发送的(hash, segment_id/None)
        self.last_sync = {}  # peer -> 最近库存交换轮次；重连重置

    @property
    def chain(self):
        """每次访问矿工自己的原 Chain。"""
        return self.miner.get_local_chain()

    def known_hashes(self):
        """完整wire已知可去重；pending/tape仍不能作为accepted父。"""
        return (
            frozenset(self.chain.block_set)
            | frozenset(self.miner.consensus.dependency_resolver.pending)
            | frozenset(
                b.blockhash
                for b in self.miner.consensus.receive_tape
                if self.miner.consensus.valid_block(b)
            )
        )

    @staticmethod
    def key(msg):
        """显示名不参与身份；Topology重传元组取其原始消息。"""
        if isinstance(msg, tuple):
            msg = msg[0]
        if isinstance(msg, DataSegment):
            return msg.origin_block.blockhash, msg.seg_id
        return msg.blockhash, None

    def connect(self, peer, round):
        """先完成两端拓扑连接，下次diffuse再交换库存，避免半连接RPC。"""
        if peer in self.nic._neighbors:
            return
        self.nic._neighbors = tuple(self.nic._neighbors) + (peer,)
        self.nic._channel_states[peer] = _IDLE
        self.nic._output_queues.setdefault(peer, [])
        self.queued.setdefault(peer, set())
        self.last_sync.pop(peer, None)

    def disconnect(self, peer):
        """保留未完成队列；被移除链路的在途消息重连后从头重试。"""
        if peer not in self.nic._neighbors:
            return
        state = self.nic._channel_states.pop(peer, _IDLE)
        if state != _IDLE:
            self.nic._output_queues[peer][0:0] = [
                m[0] if isinstance(m, tuple) else m for m in state
            ]
        self.nic._neighbors = tuple(p for p in self.nic._neighbors if p != peer)
        self.last_sync.pop(peer, None)

    def wire(self, block):
        """头/正文副本不携邻接，且几何由本次实验大小模型重新计算。"""
        if not self.miner.consensus.valid_block(block):
            return None
        wire = deepcopy(block)
        if not self.nic._network.message_preprocessing(wire):
            return None
        return wire

    def reply_getdata(self, inv):
        """反馈本地完整已知hash和缺片ID；不沿远端对象父指针取正文。"""
        result = GetDataMsg(self.miner.miner_id, inv.source, [], False)
        if inv.target != self.miner.miner_id or inv.source not in self.nic._neighbors:
            return result
        result.known_hashes = self.known_hashes()
        result.missing_segments = {
            h: tuple(sorted(p["missing"])) for h, p in self.parts.items()
        }
        if inv.isFullChain:
            result.isRequired = any(
                h not in self.chain.block_set for h in inv.frontier_hashes
            )
            return result
        obj = inv.block_or_seg
        block = obj.origin_block if isinstance(obj, DataSegment) else obj
        if not isinstance(block, Block) or self.wire(block) is None:
            return result
        if block.blockhash in result.known_hashes:
            return result
        if isinstance(obj, DataSegment):
            wire = self.wire(block)
            if type(obj.seg_id) is not int or not 0 <= obj.seg_id < wire.segment_num:
                return result
            part = self.parts.get(block.blockhash)
            result.isRequired = part is None or obj.seg_id in part["missing"]
        else:
            result.isRequired = True
        return result

    def receive(self, packet):
        """按hash收片；静态无效输入不建立缓存，完整后才进入原receive tape。"""
        self.nic._receive_buffer.append(packet)
        if not isinstance(packet.payload, list):
            block = packet.payload
            if not isinstance(block, Block):
                return {getattr(block, "name", "invalid"): False}
            wire = self.wire(block)
            accepted = wire is not None and self.miner.receive(packet.source, wire)
            if accepted:
                self.nic._network.note_dag_receive(
                    wire, self.miner.miner_id, self.miner.round
                )
            return {block.name: bool(accepted)}
        states = {}
        for segment in packet.payload:
            if not isinstance(segment, DataSegment) or not isinstance(
                segment.origin_block, Block
            ):
                continue
            block = segment.origin_block
            states.setdefault(block.name, False)
            wire = self.wire(block)
            if (
                wire is None
                or type(segment.seg_id) is not int
                or not 0 <= segment.seg_id < wire.segment_num
            ):
                continue
            h = block.blockhash
            if h in self.known_hashes():
                continue
            part = self.parts.setdefault(
                h, {"block": wire, "missing": set(range(wire.segment_num))}
            )
            part["missing"].discard(segment.seg_id)
            if part["missing"]:
                continue
            self.parts.pop(h)
            first = self.miner.receive(packet.source, part["block"])
            if first:
                self.nic._network.note_dag_receive(
                    part["block"], self.miner.miner_id, self.miner.round
                )
                states[block.name] = True
        return states

    def enqueue(self, peer, block, segment_ids=None):
        """复制有效本地块；共享祖先及重复公告不会重复排队。"""

        wire = self.wire(block)
        if wire is None or wire.isGenesis:
            return
        if self.nic._network.withSegments:
            ids = range(wire.segment_num) if segment_ids is None else segment_ids
            messages = [
                DataSegment(wire, i)
                for i in ids
                if type(i) is int and 0 <= i < wire.segment_num
            ]
        else:
            messages = [wire]
        for msg in messages:
            key = self.key(msg)
            if key not in self.queued[peer]:
                self.queued[peer].add(key)
                self.nic._output_queues[peer].append(msg)

    def send_inv(self, inv, round):
        """原网络控制调用；只在本地已连接peer之间调用。"""
        reply = GetDataMsg(require=False)
        self.nic.inv_count += 1
        self.nic._network.access_network(
            [inv, reply], self.miner.miner_id, round, inv.target
        )
        return reply

    def offer(self, peer, round):
        """frontier覆盖全accepted图；闭包差集由发送方自己的Chain计算。"""

        frontier = tuple(b.blockhash for b in self.chain.get_tips())
        inv = INVMsg(
            self.miner.miner_id, peer, None, isFullChain=True, frontier_hashes=frontier
        )
        reply = self.send_inv(inv, round)
        self.nic.sync_full_chain_count += 1
        self.last_sync[peer] = round
        if not reply.isRequired:
            return
        past = set()
        for tip in frontier:
            past.update(self.chain.get_past(tip))
        missing = past - set(reply.known_hashes)
        blocks = sorted(
            (self.chain.block_set[h] for h in missing),
            key=lambda b: (b.dag_rank, b.blockhash),
        )
        for block in blocks[: self.batch_size]:
            self.enqueue(peer, block, reply.missing_segments.get(block.blockhash))

    def forward(self, round):
        """周期对齐、hash去重、按原链路预算分批发送；控制消息不耗PoW预算。"""
        forced = bool(self.nic._forward_buffer[SELF_GEN_MSG])
        for peer in self.nic._neighbors:
            if (
                forced
                or peer not in self.last_sync
                or round - self.last_sync[peer] >= self.interval
            ):
                self.offer(peer, round)
        for origin in (OUTER_RCV_MSG, SELF_GEN_MSG):
            for block, strategy, targets in self.nic._forward_buffer[origin]:
                if not isinstance(block, Block):
                    continue
                for peer in self.nic._NICWithTp__select_target(
                    block, strategy, targets
                ):
                    self.enqueue(peer, block)
        self.nic.clear_forward_buffer()
        for peer in self.nic._neighbors:
            if self.nic._channel_states[peer] != _IDLE:
                continue
            queue = self.nic._output_queues[peer]
            send = []
            budget = (
                max(
                    1,
                    int(
                        self.nic._network.get_number_of_segments_to_send(
                            self.miner.miner_id, peer
                        )
                    ),
                )
                if self.nic._network.withSegments
                else 1
            )
            while queue and len(send) < budget:
                msg = queue.pop(0)
                payload = msg[0] if isinstance(msg, tuple) else msg
                inv = INVMsg(self.miner.miner_id, peer, payload)
                if not self.send_inv(inv, round).isRequired:
                    self.queued[peer].discard(self.key(msg))
                    continue
                send.append(msg)
            if not send:
                continue
            self.nic._channel_states[peer] = send
            self.nic._network.access_network(
                send, self.miner.miner_id, round, peer, self.nic._network.withSegments
            )
            self.nic.send_data_count += 1
            for msg in send:
                payload = msg[0] if isinstance(msg, tuple) else msg
                block = (
                    payload.origin_block
                    if isinstance(payload, DataSegment)
                    else payload
                )
                self.nic._network.note_dag_send(block, self.miner.miner_id, round)
                observer = self.miner.dag_publication_observer

                if observer is not None:
                    observer(block, round, target=peer)

    def get_reply(self, round, target, err, rest_delay=None):
        """成功释放hash请求，outage重新排队；断连后陈旧回调不复活信道。"""
        if target not in self.nic._channel_states:
            return
        state = self.nic._channel_states[target]
        if state == _IDLE:
            return
        self.nic._channel_states[target] = _IDLE
        if err == ERR_OUTAGE:
            self.nic._output_queues[target][0:0] = [
                (
                    (m[0] if isinstance(m, tuple) else m, rest_delay)
                    if rest_delay is not None
                    else (m[0] if isinstance(m, tuple) else m)
                )
                for m in state
            ]
        else:
            for msg in state:
                self.queued[target].discard(self.key(msg))
