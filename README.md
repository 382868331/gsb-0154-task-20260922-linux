# rel3vl —— 三值逻辑（3VL）下的关系查询解释器（袋语义，纯标准库）

离线、嵌入式、纯标准库的可复用 Python 模块。关系是**带重复行的有限元组袋（bag）**，
谓词采用 SQL 风格三值逻辑（TRUE / FALSE / UNKNOWN），不解析 SQL、不调用任何数据库。

本版在原批量解释器之外，新增**带行 ID 的增删流增量解释器**：增量内连接、
左外连接、分组聚合与事件时间滚动窗口，支持水位、阈值内迟到、超期拒绝、
原子批与快照恢复。

- Python：3.14.7（仅标准库；开发与验收环境为 3.14.7）
- 依赖：**无第三方依赖**；无需网络、账号、密钥、Docker 或外部服务
- 快速回归（74 用例）：`python -m unittest discover -s tests -v`（约 0.06 秒）
- 扩展验证（11 用例，含 ≥10000 行 / 100 批规模，约 10 秒）：
  `python -m unittest tests.extended_validation_checks -v`
- 演示：`python demo.py`（实测约 0.03 秒，远小于 90 秒预算，现场计算、无 sleep、无预录）

---

## 一、批量解释器（原入口，契约保持）

```python
from rel3vl import execute, QueryError

result = execute(plan, tables)                 # 旧上限：200 行/表、10 节点
result = execute(plan, tables, max_rows=rel3vl.MAX_STREAM_ROWS,
                 max_nodes=rel3vl.MAX_STREAM_NODES)   # 显式放宽（仅关键字选项）
# result = {"columns": [{"source","name"}], "rows": [[...], ...]}
```

### 计划节点

| op | 形态 | 语义 |
|---|---|---|
| `scan` | `{"op":"scan","table":"t","as":"e"}` | 扫描基表；`as` 可省（默认=表名） |
| `filter` | `{"op":"filter","input":N,"expr":E}` | 仅保留谓词为 **TRUE** 的行 |
| `project` | `{"op":"project","input":N,"columns":[{"name","table"?,"expr"}]}` | 逐表达式投影 |
| `inner_join` / `left_join` | `{"op":...,"left":N,"right":N,"on":E}` | 袋连接；左行**零** TRUE 匹配时补一行右侧全 NULL |
| `union_all` | `{"op":"union_all","left":N,"right":N}` | 袋拼接，先左后右 |
| `distinct` | `{"op":"distinct","input":N}` | `IS NOT DISTINCT FROM` 去重，保留首现 |
| **`aggregate`** | `{"op":"aggregate","input":N,"groups":[...],"aggs":[...]}` | 分组/全局聚合（见下） |
| **`window`** | `{"op":"window","input":N,"time_col":C,"size":S,"slide"?,"allowed_lateness"?}` | 事件时间滚动窗口，追加 `window_start/window_end` 两列 |

`groups`/`aggs` 的每项：`{"name": 输出列名, "table"?: "", "expr": 表达式}`；
聚合项为 `{"name","func","arg"?}`，`func ∈ {count,sum,avg,min,max}`，
`count` 可省略 `arg`（计数所有行），其余必须带 `arg`；带 `arg` 时忽略 NULL。
`groups` 缺省或为空 = **全局聚合**：即使输入为空也恰好输出一行
（`count=0`，`sum/avg/min/max=NULL`）。

窗口字段：

- `time_col`：`{"kind":"col","table":s,"column":c}`（可简写为
  `{"table":s,"column":c}`），必须是单列引用；
- `size`：正整数逻辑时间单位；`slide`：正整数且 `size % slide == 0`，
  缺省 = `size`（翻滚）；`allowed_lateness`：非负整数，缺省 0；
- 输出追加 `("", "window_start")`、`("", "window_end")` 两列（半开 `[start,end)`），
  输入不得已有同名列；
- 事件时间必须是非负 **int**（`bool`、float、负数拒绝）；滑动窗口中一行复制到
  它落入的每个窗口（袋重数）。

### 表达式

`lit / col / cmp(= <> < <= > >=) / and / or / not / is_null / coalesce`。
任一比较操作数为 NULL → UNKNOWN；逻辑量只接受 bool/NULL；不做跨类型隐式转换
（`"3"=3`、`True=1` 报 `ExpressionError`）；int/float 可混。

### 聚合数值口径（本版新增，明确列出）

- `sum`：忽略 NULL；无行或全 NULL → NULL；全 int 得 **int**，混入 float 得 float；
  `bool` 是独立类型，**不**参与 sum/avg（报错）；str 等非数值报错；
- `avg`：`sum / 非NULL行数`，按 Python `/` 得 float（如 `6/2 = 3.0`），无行 → NULL；
- `min/max`：忽略 NULL，无可比值 → NULL；比较口径与 3VL 比较一致
  （int/float 可混，bool 不做序比较、跨类型组拒绝）。

---

## 二、增量解释器（新能力，显式新入口）

```python
from rel3vl import IncrementalProcessor, StreamError

proc = IncrementalProcessor(
    plan,                                   # 同一棵计划树（新算子也可用）
    {"orders": ["oid", "cid", "ts", "amt"], # 声明输入流：表名 -> 列模式
     "customers": ["cid", "city"]})

out = proc.apply_batch({
    "watermark": 12,                        # 可选；缺省沿用当前水位
    "changes": {
        "orders":    [{"id": "o2", "op": "insert", "row": [2, None, 3, 5]}],
        "customers": [{"id": "c9", "op": "delete"}]},
})
# out = {"watermark", "batch_seq", "changes", "sealed_windows", "touched"}
rows = proc.current_result()                # 当前根结果袋（确定性全序展开）
snap = proc.dumps_snapshot()                # JSON 快照；restore/loads_snapshot 恢复
```

### 输入字段与固定缺省

- 变更项只允许 `id / op / row`：
  - `id`：非空 int/str（**bool、None、空串拒绝**），行身份不可变，改值 = 删旧 ID + 插新 ID；
  - `op`：`"insert"`（必须带与声明列等宽的 `row`）或 `"delete"`（不带 `row`；
    可携带 `row` 但必须与当前行一致，否则拒绝）；
  - 同一批内同一 id 只许出现一次；插入已存在 id / 删除未知 id 拒绝；
  - 标量经与批量入口相同的校验（NaN/Inf、不支持类型一律拒绝）。
- 批次只允许 `watermark / changes`；纯水位批可省略 `changes`。
- **水位**：非负 int，单调不减；同批**先用批前水位接收数据，再推进水位**。
- **窗口密封**：`[s,e)` 在 `e <= watermark - allowed_lateness` 时密封；
  变更一旦落入已密封窗口 → `StreamError`，**整批拒绝、状态与水位不变**。
  本批水位刚好关闭的窗口仍接收本批数据，密封通告随本批结果发布**一次**。
- 阈值内迟到（窗口尚未密封）允许；引起的每一层结果变化都输出**撤回 `-` 与新增 `+`**。

### 输出状态

- `changes`：根结果袋差量，每项 `{"op": "+"|"-", "row": [...], "mult": n}`；
  同批固定**先全部 `-` 后全部 `+`**，同号内按行全序排序（确定性，可逐字节复现）。
  全局聚合的初始零行是初始结果的一部分，首批会以 `−[0,…] +[…]` 形式变化。
- `sealed_windows`：本批新密封窗口 `[{"node","window_start","window_end"}]`，
  只发布一次，重复水位推进不重复发布。
- `touched`：每个节点实际触碰的状态键（等值键桶 `left_keys/right_keys`、
  `pad_switched`、聚合 `groups`、窗口 `window_starts/events`、受影响行/配对身份），
  用于核对**未对无关组全量扫描**（见 `INCREMENTAL_SEMANTICS.md` 的规则与证明例）。
- `current_result()`：物化为 list 行；`dumps_snapshot()/loads_snapshot()`：
  快照恢复后继续处理同一批输入，得到相同变更流。

### 支持范围（本版实测）

- 单流累计 ≥ **10000 输入行**、**100 批**更新（扩展验证中 10070 行 / 100 批
  / 三算子连接+窗口+聚合，流式处理 < 1 秒，末态与批量解释器逐组一致）；
- 重复内容重数 **100**（删一份只减一份乘积）；NULL 连接键不匹配；
- **三算子连接计划**：`left_join → inner_join → window → aggregate`；
- 逻辑水位（整数），无物理时钟。

---

## 三、错误体系

所有输入错误抛 `QueryError`（`ValueError` 子类）：
`PlanError / SchemaError / TableError / ExpressionError`（批量）与
**`StreamError`**（增量：未知/重复 ID、水位倒退、超保留期限、批次结构非法等）。
增量批采用"先只读校验/试算、后一次性提交"，任何 `StreamError` 都不留部分变更。

错误定位前缀保持不变：`[plan]`、`[node#k]`、`[node#k/expr]`、`[table:名字]`；
增量错误额外用 `[stream:表名] id=...` 定位到流、行 ID。

## 四、与旧行为的差异及迁移

旧行为全部保留，新增能力通过**新算子**与**新入口/显式关键字选项**提供，
旧调用 `execute(plan, tables)` 签名、异常、确定顺序与 200 行/10 节点上限不变。
枚举新增点（无一条需要改动旧代码）：

1. 计划新增 `aggregate`、`window` 两个 `op`（旧计划不含即完全不受影响）；
2. `execute` 新增**仅关键字**参数 `max_rows`/`max_nodes`，缺省值等于旧上限；
   需要处理更大输入（含新算子）时显式传入 `MAX_STREAM_ROWS`/`MAX_STREAM_NODES`；
3. `validate_tables(tables, max_rows=...)` 新增同名关键字；
4. 新增异常类型 `StreamError`（`QueryError` 子类），旧 `except QueryError` 代码
   无需修改即可同时接住它。

## 五、设计取舍（要点）

1. **袋重数与行 ID**：重复行按不同 ID 各计一重；join 输出身份是左右 ID 对，
   外连接补 NULL 行身份是 `(左id, None)`，匹配数 0↔正 时精确撤回/补出。
2. **差量而非追加**：聚合、连接、窗口的任何值变化都归约为受影响身份的
   `−旧 +新`；迟到更新不可能只追加一条结果。
3. **原子批**：全部校验与差量在只读叠加层完成，提交阶段不做可失败操作。
4. **不等值 ON 与等值 ON**：等值 AND 链自动哈希分桶（输出顺序与嵌套循环逐对
   一致，跨类型异常条件一致，见 `test_batch_new_ops`）；其余 ON 保留嵌套循环。
5. **确定性**：结果与 changelog 不依赖 dict 迭代/字符串哈希随机化；桶按身份
   全序有序，distinct 代表取最小存活成员 ID（对齐批量"首现行"）。
6. **无算术运算**：除聚合累加外模块不产生新数值；比较用机器 int / IEEE-754
   binary64，不设容差；聚合数值口径见上。

差量传播规则、外连接嵌套聚合再接窗口的**重数证明例**与触碰键凭据见
[`INCREMENTAL_SEMANTICS.md`](./INCREMENTAL_SEMANTICS.md)。

## 六、测试与验证

- `python -m unittest discover -s tests -v`
  快速回归（原 38 用例一条未删、断言未放松；新增流式 23 用例 + 新算子 13 用例，共 74）；
- `python -m unittest tests.extended_validation_checks -v`
  扩展验证：独立朴素参考（`tests/_fixtures.py`，**不 import 被测核心**，
  期望值不调用被测求解器）对 aggregate/window 完整小域交叉核对；增量 vs
  原批量解释器的逐批累计对照；≥10000 行/100 批、重数 100；触碰键只含相关组；
  快照重放一致；约 10 秒。
- `python demo.py`：A) 旧 3VL/ON-vs-WHERE；B) 外连接→聚合→窗口端到端
  （同批删除最后匹配 + 阈值内迟到行 + 水位关闭，只发布一次，触碰键，
  现场调批量解释器对照）；C) 超保留期限更新被真实拒绝且状态不变。

## 七、文件

```
rel3vl/__init__.py      # 公开 API（execute / IncrementalProcessor / 错误 / 上限）
rel3vl/errors.py        # QueryError 体系（含 StreamError）
rel3vl/values.py        # 标量校验、IS NOT DISTINCT FROM
rel3vl/expressions.py   # 3VL 表达式编译/求值
rel3vl/aggregation.py   # 批量/增量共用的可维护聚合累加器
rel3vl/windows.py       # 窗口规格校验与窗口起点分配（批量/增量共用）
rel3vl/engine.py        # execute(plan, tables)、计划编译、aggregate/window、等值哈希连接
rel3vl/streaming.py     # IncrementalProcessor：增删流、水位、快照、触碰键记录
tests/test_engine.py            # 原 38 用例（未删改）
tests/test_streaming.py         # 增量语义 23 用例（手工差量 + 逐批批量对照）
tests/test_batch_new_ops.py     # aggregate/window/等值连接等价性 13 用例
tests/extended_validation_checks.py  # 扩展验证（规模/重数/触碰键/朴素参考）
tests/_fixtures.py              # 固定数据 + 独立朴素参考（不 import 核心）
demo.py                         # 固定输入演示（旧能力 + 三组新能力 + 拒绝）
INCREMENTAL_SEMANTICS.md        # 差量传播规则与重数证明例
```

## 八、已知限制

- 不是 SQL：不解析 SQL 文本；聚合仅 5 个内置函数、无 `GROUPING SETS`/
  `HAVING`/排序/子查询；窗口只按事件时间翻滚/滑动，无会话/会话超时；
- 事件时间固定为非负整数逻辑时钟；不支持基于处理时间的触发；
- 非等值 ON 的增量维护退化为嵌套循环（等值 AND 链才走索引），
  大规模计划应使用等值连接（题面场景即如此）；
- join 为两路；`union_all` 增量身份不跨同名流去重；
- 快照只含基表与水位/批次/密封前沿，派生状态靠确定性重建（恢复成本与当前规模线性）。
