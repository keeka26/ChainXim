# Conflux PoW DAG协议规则

实现参考《Scaling Nakamoto Consensus to Thousands of Transactions per Second》2018 v4 Fig.3–6。节点只处理本地已接纳图。

- 主父组成树，reference组成额外依赖边；区块头不重复列出全部历史。
- pivot从创世沿最重主父子树选择，单位区块权重，并列按hash确定；reference不增加树权重。
- Past包含自身以及全部主父和reference祖先。epoch_i=Past(B_i)−Past(B_{i−1})。
- epoch内部按依赖拓扑排序：一批ready块全部按hash输出后，再释放下一批；各epoch顺序拼接。
- 不在pivot尾Past中的accepted块保持未分配epoch，不能自动丢弃。
- 缺任一父的块进入resolver.pending；父到达后级联接纳；pending不参与诚实选父。
- 挖矿选择本地pivot尾及其余全部accepted tips，每节点每轮成功即停，最多一个普通块。
- pivot_depth确认截取满足P−i≥N的epoch前缀；这是可撤销经验规则，不实现论文风险概率或终局。

完整接口、轮顺序和输入输出见[开发指南](developer-guide.dag.zh.md)。
