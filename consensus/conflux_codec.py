"""Conflux v2 仿真头的纯编码、SHA-256及身份检查。

不维护图，不执行挖矿，不访问全局配置；统一定义本项目v2头的规范编码。
这不是Conflux主网wire格式。旧PoW的v1编码留在原共识类中。
"""

from typing import TYPE_CHECKING

from functions import hash_bytes

if TYPE_CHECKING:
    from data import Block, BlockHead

HEADER_VERSION = 2
HASH_LENGTH = 32
DOMAIN = b"CHAINXIM-CONFLUX\0"
ZERO_HASH = b"\x00" * HASH_LENGTH
MAX_TARGET = b"\xff" * HASH_LENGTH


def _fixed_hash(value, name: str) -> bytes:
    """哈希/目标只能是固定32字节bytes，不把hex字符串当消息。"""
    if not isinstance(value, bytes) or len(value) != HASH_LENGTH:
        raise ValueError(f"{name} must be exactly 32 bytes")
    return value


def _integer(value, name: str, width: int, *, signed: bool = False) -> int:
    """编码前核查整数宽度，bool和浮点不作为协议整数。"""
    low = -(2 ** (width - 1)) if signed else 0
    high = 2 ** (width - 1) - 1 if signed else 2**width - 1
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not low <= value <= high
    ):
        raise ValueError(f"{name} must be an integer in [{low}, {high}]")
    return value


def normalize_refhashes(prehash: bytes, refhashes) -> tuple[bytes, ...]:
    """发送端构造规范引用tuple；重复或与主父同角色冲突时拒绝。

    接受list/tuple以便本地模板构造，不修改输入；不默默去重非法声明。
    """
    _fixed_hash(prehash, "prehash")
    if not isinstance(refhashes, (list, tuple)):
        raise ValueError("refhashes must be a list or tuple of 32-byte hashes")
    refs = tuple(_fixed_hash(value, "refhashes") for value in refhashes)
    if len(set(refs)) != len(refs) or prehash in refs:
        raise ValueError("refhashes must be unique and must exclude prehash")
    _integer(len(refs), "ref_count", 32)
    return tuple(sorted(refs))


def encode_header(head: "BlockHead") -> bytes:
    """规范编码完整v2头；验证后返回bytes，不改写头或声明ID。

    构造时可排序引用，收到的头必须已经是规范tuple；非法次序不在这里修正。
    timestamp为逻辑轮，target和所有父角色均进入承诺。
    """
    version = _integer(getattr(head, "header_version", None), "header_version", 16)
    if version != HEADER_VERSION:
        raise ValueError("header_version must be 2 for the Conflux codec")
    miner = _integer(getattr(head, "miner", None), "miner", 32, signed=True)
    timestamp = _integer(getattr(head, "timestamp", None), "timestamp", 64)
    prehash = _fixed_hash(getattr(head, "prehash", None), "prehash")
    refs = getattr(head, "refhashes", None)
    normalized = normalize_refhashes(prehash, refs)
    if not isinstance(refs, tuple) or refs != normalized:
        raise ValueError("refhashes must already be a sorted canonical tuple")
    target = _fixed_hash(getattr(head, "target", None), "target")
    content = getattr(head, "content", None)
    if not isinstance(content, bytes):
        raise ValueError("content must be bytes")
    _integer(len(content), "content_length", 32)
    nonce = _integer(getattr(head, "nonce", None), "nonce", 32)
    if prehash == ZERO_HASH:
        if (
            miner != -1
            or timestamp != 0
            or refs
            or target != MAX_TARGET
            or content != b""
            or nonce != 0
        ):
            raise ValueError("genesis must use the fixed Conflux v2 fields")
    elif miner < 0:
        raise ValueError("miner must be nonnegative outside the fixed genesis")
    return (
        DOMAIN
        + version.to_bytes(2, "big")
        + miner.to_bytes(4, "big", signed=True)
        + timestamp.to_bytes(8, "big")
        + prehash
        + len(refs).to_bytes(4, "big")
        + b"".join(refs)
        + target
        + len(content).to_bytes(4, "big")
        + content
        + nonce.to_bytes(4, "big")
    )


def calculate_header_hash(head: "BlockHead") -> bytes:
    """只计算SHA-256；不调用会覆盖block.blockhash的旧块方法。"""
    return hash_bytes(encode_header(head)).digest()


def is_genesis_header(head: "BlockHead") -> bool:
    """先检查完整规范，再判固定创世；无本地父对象不等于创世。"""
    encode_header(head)
    return head.prehash == ZERO_HASH


def validate_block_header(block: "Block") -> bool:
    """非修改式检查头/声明hash/创世标记/自依赖，失败返回False。

    这不是PoW或DAG有效性判定：不检查hash<target、配置目标、父块存在、
    普通块height/rank或矿工ID是否在本次网络中。它们由依赖接纳和共识验证处理。
    """
    try:
        head = block.blockhead
        claimed = _fixed_hash(block.blockhash, "blockhash")
        payload = encode_header(head)
        if claimed == head.prehash or claimed in head.refhashes:
            return False
        if claimed != hash_bytes(payload).digest():
            return False
        genesis = head.prehash == ZERO_HASH
        if not isinstance(block.isGenesis, bool) or block.isGenesis != genesis:
            return False
        if genesis and not (
            type(block.height) is int
            and block.height == 0
            and type(block.dag_rank) is int
            and block.dag_rank == 0
        ):
            return False
        return True
    except (ValueError, TypeError, AttributeError, OverflowError):
        return False
