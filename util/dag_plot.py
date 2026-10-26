"""只读双视角DAG绘图：pivot、epoch、旁支。

读取调用方Chain已安装的protocol_view，不重算共识、不改变节点状态。
全局生成图上的pivot/epoch仅供全知观察，不表示全网达成一致。
"""

from html import escape
from pathlib import Path
from graphviz import Digraph

PIVOT_FILL = "#1e3a8a"
SIDE_FILL = "#ffedd5"
UNASSIGNED_FILL = "#e5e7eb"
PIVOT_EDGE = "#2563eb"
TREE_EDGE = "#64748b"
REFERENCE_EDGE = "#a8a29e"


def build_dag_graph(chain, *, scope, miner_id=None):
    """构建DOT对象。epoch从0开始；标签依据本次指定图的view。

    scope为global_generated或node_accepted；分层依据dag_rank，全部块/边均保留。
    绘图节点miner_id只用于标题，不能影响pivot选择、排序或停止判断。
    """
    if scope not in ("global_generated", "node_accepted"):
        raise ValueError("DAG plot scope must be global_generated or node_accepted")
    title = (
        "Global generated DAG (observer)"
        if scope == "global_generated"
        else f"Node {miner_id} accepted DAG (local view)"
    )
    view = chain.protocol_view
    pivot = set(view.pivot) if view else set()
    epoch_of = (
        {h: index for (index, epoch) in enumerate(view.epochs) for h in epoch}
        if view
        else {}
    )
    pivot_edges = set(zip(view.pivot[1:], view.pivot[:-1])) if view else set()
    legend = f'<<TABLE BORDER="0" CELLBORDER="0" CELLPADDING="3">\n    <TR><TD COLSPAN="2"><B>{escape(title)}</B></TD></TR>\n    <TR><TD BGCOLOR="{PIVOT_FILL}" WIDTH="20"> </TD><TD ALIGN="LEFT">Pivot chain</TD></TR>\n    <TR><TD BGCOLOR="{SIDE_FILL}"> </TD><TD ALIGN="LEFT">Ordered side block</TD></TR>\n    <TR><TD BGCOLOR="{UNASSIGNED_FILL}"> </TD><TD ALIGN="LEFT">Not in current pivot Past</TD></TR>\n    <TR><TD COLSPAN="2">Solid: main parent; dashed: reference</TD></TR>\n    <TR><TD COLSPAN="2">Blue edges: pivot path; child &#8594; dependency</TD></TR>\n    <TR><TD COLSPAN="2">Epoch/order: provisional, not confirmation</TD></TR>\n    </TABLE>>'
    dot = Digraph("Conflux", format="svg")
    dot.attr(
        rankdir="BT",
        label=legend,
        labelloc="t",
        fontname="Helvetica",
        fontsize="10",
        nodesep="0.35",
        ranksep="0.65",
        splines="polyline",
    )
    dot.attr(
        "node",
        shape="box",
        style="rounded,filled",
        fontname="Helvetica",
        fontsize="10",
        margin="0.12,0.08",
    )
    dot.attr("edge", arrowsize="0.6")
    blocks = sorted(chain.block_set.values(), key=lambda b: (b.dag_rank, b.blockhash))
    ranks = {}
    for block in blocks:
        h = block.blockhash
        role = "pivot" if h in pivot else "side" if h in epoch_of else "unassigned"
        fill = {"pivot": PIVOT_FILL, "side": SIDE_FILL, "unassigned": UNASSIGNED_FILL}[
            role
        ]
        epoch = str(epoch_of[h]) if h in epoch_of else "unassigned"
        label = (
            f"{block.name}\nheight={block.height} rank={block.dag_rank}\nepoch={epoch}"
        )
        dot.node(
            h.hex(),
            label,
            fillcolor=fill,
            fontcolor="white" if role == "pivot" else "#111827",
            color=PIVOT_FILL if role == "pivot" else "#94a3b8",
            group="pivot" if role == "pivot" else "",
            tooltip=f"hash={h.hex()}; miner={block.blockhead.miner}; round={block.blockhead.timestamp}",
            _attributes={"class": role + " honest"},
        )
        ranks.setdefault(block.dag_rank, []).append(h.hex())
    for rank, hashes in sorted(ranks.items()):
        with dot.subgraph(name=f"rank_{rank}") as layer:
            layer.attr(rank="same")
            for h in hashes:
                layer.node(h)
    for block in blocks:
        if block.isGenesis:
            continue
        h, parent = (block.blockhash, block.blockhead.prehash)
        on_pivot = (h, parent) in pivot_edges
        dot.edge(
            h.hex(),
            parent.hex(),
            style="solid",
            color=PIVOT_EDGE if on_pivot else TREE_EDGE,
            penwidth="2.4" if on_pivot else "1.0",
            weight="5" if on_pivot else "1",
        )
        for ref in block.blockhead.refhashes:
            dot.edge(
                h.hex(), ref.hex(), style="dashed", color=REFERENCE_EDGE, penwidth="0.8"
            )
    return dot


def draw_dag_graph(chain, directory, *, filename, scope, miner_id=None, show=False):
    """保存DOT与SVG；show只控制打开文件，不决定输出范围或节点。"""
    dot = build_dag_graph(chain, scope=scope, miner_id=miner_id)
    return Path(
        dot.render(
            filename=filename, directory=str(directory), view=show, cleanup=False
        )
    )
