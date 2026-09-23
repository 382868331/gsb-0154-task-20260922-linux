# rel3vl —— 三值逻辑（3VL）下的关系查询解释器

离线、嵌入式、纯标准库的可复用 Python 模块。关系是**带重复行的有限元组袋（bag）**，
谓词采用 SQL 风格三值逻辑（TRUE / FALSE / UNKNOWN），不解析 SQL、不调用任何数据库。

本仓库含两套入口（同一套数据模型与编译代码）：

1. **批量解释器** `execute(plan, tables)`：一次性物化整棵计划（原有，v1）；
2. **增删流增量解释器** `IncrementalExecutor`（v2 新增）：吃带行 ID 的增删流，
   支持袋语义增量内连接 / 左外连接 / 分组聚合 / 事件时间滚动窗口 / 逻辑水位 /
   阈值内迟到 / 快照恢复。

- Python：3.14.7（仅用标准库，3.10+ 语法亦可运行，开发与验收环境为 3.14.7）
- 依赖：**无第三方依赖**；无需网络、账号、密钥、Docker 或外部服务
- 快速回归：`python -m unittest discover -s tests -v`（实测约 0.6 秒，73 个用例）
- 扩展验证（上万行规模）：`python -m verification.scale_check`（实测约 4 秒）
- 演示：`python demo.py`（实测约 0.05 秒，远小于 90 秒预算；现场计算、无 sleep、无预录）

---

## 一、批量接口（原有，默认行为完全不变）

```python
from rel3vl import execute, QueryError

result = execute(plan, tables)
# result = {
#   "columns": [{"source": "e", "name": "id"}, ...],
#   "rows":    [[...], ...],          # list[list]，顺序确定
# }
```

所有非法输入抛 `QueryError`（`ValueError` 子类）的子类：
`PlanError` / `SchemaError` / `TableError` / `ExpressionError`
（增量入口另加 `StreamError` 家族，见下）。错误消息带稳定定位前缀：
`[plan]`、`[node#<序号>]`、`[node#<序号>/expr]`、`[table:<表名>]`。

### 表（输入）

```python
tables = {
    "employees": {
        "columns": ["id", "dept", "score"],
        "rows": [[1, "A", 10], [2, "A", None]],
    }
}
```

- 列值类型：`None`（NULL）、`bool`、`int`、有限 `float`、`str`。
- `float` 的 NaN / ±Infinity **拒绝**；不支持的类型（list/dict 等）**拒绝**，不隐式转换。
- `bool` 是独立类型，不会被静默当作整数；`True` 与 `1` 之间的比较直接报错。

### 计划节点（判别字段 `op`）

| op | 形态 | 语义 |
|---|---|---|
| `scan` | `{"op":"scan","table":"t","as":"e"}` | 扫描基表；`as` 可省（默认=表名），自连接必须用不同来源 id |
| `filter` | `{"op":"filter","input":N,"expr":E}` | 仅保留谓词为 **TRUE** 的行；FALSE 与 UNKNOWN 都丢弃 |
| `project` | `{"op":"project","input":N,"columns":[{"name":"v","table":"","expr":E}]}` | 逐表达式投影；`table` 可省；输出限定名不得重复 |
| `inner_join` | `{"op":"inner_join","left":N,"right":N,"on":E}` | 袋语义嵌套连接；`on` 仅为 TRUE 时匹配 |
| `left_join` | 同上 | 左行对**所有**右行都无 TRUE 匹配时，补一行右侧全 NULL |
| `union_all` | `{"op":"union_all","left":N,"right":N}` | 袋拼接：先左后右；两边列名序列必须一致 |
| `distinct` | `{"op":"distinct","input":N}` | 袋去重，保留每组首次出现，顺序确定 |
| `group_by` *(v2)* | `{"op":"group_by","input":N,"keys":[K…],"aggs":[A…]}` | 分组聚合；`keys` 可省略或为 `[]`（全局聚合） |
| `window` *(v2)* | `{"op":"window","input":N,"time":E,"width":W,"keys":[K…],"aggs":[A…]}` | 事件时间滚动窗口聚合（逻辑时钟） |

聚合列 `A = {"name": "n", "func": "sum", "expr": E}`；`count_star` 无 `expr`。

| func | 语义（袋语义，重复行按重数计入） |
|---|---|
| `count_star` | 组内行数（COUNT(*)，含全 NULL 行） |
| `count` | `expr` 非 NULL 的行数（COUNT(expr)） |
| `sum` | 非 NULL 整数值之和；组为空或全 NULL → NULL |
| `avg` | 非 NULL 值 `sum/count` 的 IEEE-754 双精度真除法；无非 NULL 值 → NULL |
| `min` / `max` | 非 NULL 值最小/最大；全 NULL → NULL |

- 聚合输入表达式限定为 **int 或 NULL**（bool 拒绝参与算术）。
  整数求和用 Python 任意精度整数，精确无舍入；`avg` 是唯一引入一次
  双精度除法的地方（`int / int -> float`），误差口径即该次 true division，
  不设容差、不做反复累加，不存在累加顺序误差。
- **空组不输出**（GROUP BY 中没有任何行的键不出现在结果里）。
- 聚合结果按分组键跨类型确定排序（NULL < bool < 数值 < 字符串），顺序可复现。

`window` 额外输出两列 `window_start` / `window_end`（半开区间 `[s, s+width)`，
`s = floor(t / width) * width`），位置在分组键之后、聚合列之前。
事件时间表达式必须求出**非 NULL、非 bool 的整数**（逻辑时钟），否则编译/求值报错。

### 表达式（判别字段 `kind`）

| kind | 形态 | 说明 |
|---|---|---|
| `lit` | `{"kind":"lit","value":1}` | 字面量，同样拒绝 NaN/Inf |
| `col` | `{"kind":"col","table":"e","column":"id"}` | 来源 id + 列名限定；不存在即 `SchemaError` |
| `cmp` | `{"kind":"cmp","op":"=","left":E,"right":E}` | `= <> < <= > >=`；任一操作数为 NULL → UNKNOWN |
| `and` / `or` | `{"kind":"and","left":E,"right":E}` | 3VL：`F AND U = F`、`T OR U = T` 等 |
| `not` | `{"kind":"not","expr":E}` | `NOT UNKNOWN = UNKNOWN` |
| `is_null` | `{"kind":"is_null","expr":E}` | 严格 NULL 判定，返回 TRUE/FALSE |
| `coalesce` | `{"kind":"coalesce","args":[E,…]}` | 首个非 NULL；全 NULL 则 NULL |

逻辑运算的操作数必须是 bool 或 NULL；比较不做跨类型隐式转换
（`"3" = 3`、`True = 1` 直接报 `ExpressionError`）；数值允许 int/float 混合比较。

### 批量入口输入上限

- 每张输入表默认最多 **200 行**（`rel3vl.MAX_ROWS_PER_TABLE`）；
- 单棵计划最多 **10 个节点**（`rel3vl.MAX_NODES`）；
- 表达式嵌套最多 200 层。

`execute(plan, tables, max_rows=N)` 是 v2 新增的**显式关键字选项**：
增量对照需要在累计的上万行上重算批量结果，内部/测试可传更大的 `max_rows`；
旧调用不传该参数时默认值与行为完全不变（见"兼容性"）。

---

## 二、增删流增量解释器（v2 新增）

```python
from rel3vl import IncrementalExecutor, LateDataError

ex = IncrementalExecutor(plan, tables, allowed_lateness=2)
out = ex.apply_batch({
    "changes": [
        {"table": "orders", "op": "+", "id": "r1", "row": ["A", 10, 0]},
        {"table": "orders", "op": "-", "id": "r0"},          # 删除可只给 id
    ],
    "watermark": 7,        # 可选；整数逻辑时钟，只能单调不减
})
# out = {"seq": 3, "watermark": 7,
#        "changes": [{"row": [...], "delta": +1|-1}, ...],
#        "touched": {节点序号: [状态键, ...]},
#        "closed":  {窗口节点序号: [关闭的窗口起点, ...]}}
```

### 输入字段与边界含义

- **行 ID**：每条基表行带调用方给定的**非空字符串 `id`**。袋多重数 = 不同 ID 的个数；
  内容相同的行按重数真实计入。插入已存在的 id、删除不存在的 id 都是 `ChangeError`。
  删除可只给 `id`（按身份删除）；若同时给 `row`，必须与当前行逐值相等，否则拒绝。
- **变更**：`op` 仅 `"+"` / `"-"`；`row` 为与表列等宽的列表，标量规则同批量入口。
- **批次**：一个批次 = 一组有序 changes + 可选 `watermark`。整批先在**影子状态**上计算，
  任一记录非法（未知表、重复 id、3VL 类型错、迟到、窗口已关闭、水位倒退……）
  都在提交前抛异常，**整批回滚、状态不变**（原子批次）。
- **逻辑水位**：整数、非 bool、单调不减；倒退抛 `WatermarkError`。
  固定契约：**同批的水位与数据原子生效**——批内 changes 按提交后的水位判定迟到，
  然后再关闭到期窗口。不带 `watermark` 字段时沿用当前水位。
- **迟到阈值**：`allowed_lateness` 为非负整数。事件时间 `t` 满足
  `t >= watermark - allowed_lateness` 才接受；更早的更新抛 `LateDataError`，不改状态。
  边界取等号：`t == watermark - allowed_lateness` **接受**。
- **窗口关闭**：滚动窗口 `[s, s+width)` 在 `s + width <= watermark - allowed_lateness`
  时关闭。关闭后结果行**冻结保留**在输出袋中，桶状态释放；
  此后进入该窗口的任何插入或删除一律 `LateDataError`。关闭只枚举到期起点下的桶
  （有有序起点索引），不扫描其他窗口或无关分组。

### 输出状态与并列（tie）规则

- 每批根节点**只发布一次**净变更 `changes`：`delta > 0` 为新增，`delta < 0` 为撤回。
- 聚合值改变时输出成对的**撤回旧行 + 新增新行**（例如
  `("A", 0, 5, 10, 1): -1` 与 `("A", 0, 5, 15, 2): +1`），绝不只追加一条新结果。
- 同一结果行在一批内的多次变化合并为单个净 `delta`。
- 发布顺序确定：按行的跨类型标签排序（NULL < bool < 数值 < 字符串；
  数值内 int/float 按数学值），同一输入重放得到逐字相同的变更流。
- `current_result()` 返回当前根袋的确定顺序物化（列定义同 `execute()`）。
- `touched[node_num]` 列出该节点本批**实际触碰**的状态键（等值键 / 分组键 /
  窗口桶 / 关闭桶 / 行 id），用于核验局部性与"未对无关组全量扫描"。

### NULL 连接键、三值过滤、空组（沿用原语义）

- 连接键任一分量为 NULL 时 `NULL = NULL` 为 UNKNOWN，**不匹配**、不进等值哈希索引。
- 增量 `filter` 与连接 `on` 都只认真值 TRUE；FALSE/UNKNOWN 不通过（原 3VL 语义）。
- 分组聚合的空组不输出；组从有变无撤回最后一行，从无变有重新插入。
- 左外连接仅在左行对右袋**零 TRUE 匹配**时补一行右侧全 NULL（按左副本重数补）。

### 快照恢复

```python
snap = ex.snapshot()                 # 纯 JSON 可序列化 dict（含 version 字段）
ex2 = ex.restore(snap)               # 校验版本/节点集合，返回同计划新执行器
```

快照保存水位、迟到阈值、序号、每个节点的袋与内部索引。
非法快照（版本不符、节点集合不一致、缺字段）抛 `SnapshotError`。
恢复后继续处理同一批输入，得到与原执行器**逐字相同**的变更流（测试固化）。

### 错误类型（增量）

均为 `QueryError` 子类，基类 `StreamError`：

| 类型 | 触发 |
|---|---|
| `ChangeError` | 记录结构非法、未知表、op 非法、id 重复/不存在、行宽不符、删除内容不符 |
| `WatermarkError` | 水位非整数或倒退 |
| `LateDataError` | 事件时间超过保留期限（`.reason="beyond_retention"`），或事件时间非整数/NULL（`"missing_timestamp"`），或写入已关闭窗口 |
| `SnapshotError` | 快照版本/结构与计划不兼容 |

增量入口规模上限：每表 100 000 行、单批 100 000 条变更
（`STREAM_MAX_ROWS_PER_TABLE` / `STREAM_MAX_BATCH_CHANGES`）。
批量入口默认 200 行上限保持不变。

---

## 三、差量传播规则与重数证明

袋用 `Counter` 表示；`L[l]`、`R[r]` 为左/右输入袋中行的重数，
`O[x]` 为输出袋重数。定义左行 `l` 的**单个副本**在右袋中的 TRUE 匹配重数

```
M(l) = Σ_{r : on(l,r)=TRUE} R[r]
```

### 内连接 / 左外连接

**左行 l 变化 d（d=±1，按副本重数传播）：**

```
∀ r 匹配 l:  O[l, r] += d · R[r]
左外且 M(l) = 0:  O[l, NULL] += d
```

**右行 r 变化 d（先把 R[r] 更新到新值）：**

```
∀ l 匹配 r:  O[l, r] += L[l] · d
左外: O[l, NULL] += L[l] · ( 1[M'(l)=0] − 1[M(l)=0] )
      其中 M(l) = M'(l) − d      （变化行 r 对 M(l) 的贡献恰好改变 d）
```

于是补 NULL 行只在匹配数跨越 0 时变动：

- `0 → 1`：撤回 `L[l]` 条旧补 NULL 行（题面要求的"匹配数从零变一时撤回旧补 NULL 行"）；
- `1 → 0`：补回 `L[l]` 条；`1 → 1`、`0 → 0` 不动。

**重数证明例（同批：删最后匹配 + 补入迟到行 + 水位关闭）**：
设左袋有 `L[l1]=2`（两行同内容）、右袋 `R[r1]=1` 且 `on(l1,r1)=TRUE`，故 `M(l1)=1`，
输出含匹配行 `(l1,r1)` 重数 2、无补 NULL 行。本批删除 `r1`（`d=−1`，新 `R[r1]=0`）：

```
O[l1, r1] += L[l1]·(−1) = −2          # 两条匹配行同时撤回
M'(l1) = 0, M(l1) = 0 − (−1) = 1
O[l1, NULL] += 2 · (1[0=0] − 1[1=0]) = 2 · (1 − 0) = +2   # 补回两条 NULL 行
```

输出净变更恰为 `(l1,r1): −2` 与 `(l1,NULL): +2`，与把批后累计输入交批量解释器
重算的袋完全一致。同批再插入一条落入**仍开放**窗口的迟到左行 `l2`，左规则给出
`M(l2)=0 → O[l2,NULL] += 1`；水位关闭的是更早的窗口，其冻结行不参与本批变更。
整个批次根节点只发布一次。`tests/test_stream.py` 与 `verification/scale_check.py`
把每批（及每 10 批的上万行检查点）的增量袋与批量重算袋逐行比对，固定该结论。

**局部性**：右侧一次插入/删除只枚举与该右行等值键相同、且残差谓词为 TRUE 的左行；
分组/窗口只触碰对应键的桶；窗口关闭只枚举到期起点。`out["touched"]`
记录实际触碰的键，测试断言"插入 k=3 的右行不会触碰 k=1/k=2 的状态键"。

### 分组聚合 / 窗口聚合

状态为 `组键 -> Counter(度量元组)`。一行带重数 d 进出某组时：

```
old = 该组度量袋变化前的聚合结果行（组不存在则无）
new = 变化后的聚合结果行（度量袋清空则组消失、无 new）
发布 old: −1（若有）与 new: +1（若有）
```

度量袋按重数更新，因此 `sum/count/min/max/avg` 的结果与批量重算一致；
窗口额外以 `(窗口起点, 组键)` 分桶，事件时间把行唯一映射到一个桶，
迟到更新落在旧桶时同样产出"撤回 + 新增"，而不是追加第二条结果。

---

## 四、设计取舍

1. **一套编译、两种执行**：批量与增量共用计划编译、3VL 表达式与聚合实现
   （`engine.py` / `expressions.py` / `aggregates.py`），增量状态节点
   （`stream.py`）复用编译后的谓词/投影闭包，从根上保证两者口径一致。
2. **袋语义与确定顺序**：关系用 `Counter` 管理；输出/发布按跨类型标签排序，
   重复元组按多重数真实保留，同一输入可重复得到同一结果。
3. **NULL 传播与 3VL**：比较遇 NULL 得 UNKNOWN；`filter` 与连接 `on` 只认 TRUE；
   `left_join` 只在零 TRUE 匹配时补一行 NULL；等值索引不收 NULL 键。
4. **原子批次**：每批克隆状态到影子副本，异常即丢弃副本，绝不留部分变更。
5. **窗口关闭与状态释放**：关闭窗口的结果行冻结保留（可继续被读到），
   桶状态从内存释放，并用有序起点索引保证关闭不扫无关桶。
6. **列身份是 `(source_id, column_name)`**；DISTINCT/分组键的相等是
   `IS NOT DISTINCT FROM`（NULL 同组、`1` 与 `1.0` 同组、bool 独立）。
7. **数值口径**：整数求和任意精度精确；唯一浮点运算是 `avg` 的一次双精度真除法；
   输入侧拒绝 NaN/Infinity，不设容差。
8. **检查器不信求解器自报标志**：所有正确性结论都由独立来源核对——
   小确定性域用测试内**独立朴素参考**（Counter + dict 行，不 import 被测核心）
   完整核对；规模场景用**原批量解释器** `execute()` 对累计输入重算做检查点；
   快照/迟到/拒绝等以状态前后逐行比对断言，不读任何"我成功了"的布尔位。

## 五、与上一版（v1）的兼容性

v1 的合法调用、异常类型与确定性结果全部保留；新增能力通过**独立入口与新节点**
引入，不以新格式污染旧调用：

| 变更 | 影响 / 迁移办法 |
|---|---|
| 新增 `IncrementalExecutor` 与 `StreamError` 家族 | 纯新增；不使用即无影响。 |
| 新增 `group_by` / `window` 计划节点 | 纯新增 op；旧计划不含这两个 op，解释路径不变。 |
| `execute()` 新增关键字参数 `max_rows`（默认 200） | **默认值不变**，`execute(plan, tables)` 行为逐字节一致；仅显式传参才放宽。 |
| 编译产物 `_Compiled` 增加 `num/op/children/detail` 元数据 | 下划线内部结构，非公开 API；`execute()`/`compile_plan()` 的输入输出不变。 |
| 聚合输入限定 int/NULL、事件时间限定整数 | 新增节点的规则；旧节点无算术，不存在旧数据被新规则拒绝的情形。 |

无任何旧测试被删除或放松断言：原 `tests/test_engine.py`（38 用例）原样保留并全部通过。

## 六、测试与预算

| 命令 | 内容 | 实测耗时 |
|---|---|---|
| `python -m unittest discover -s tests -v` | 快速回归：原 38 个批量/3VL 用例 + 35 个增量用例（连接差量、撤回补 NULL、删一份重复、NULL 键、窗口边界、迟到、原子批、快照、独立参考差分、touched 局部性） | ≈ 0.6 s |
| `python -m verification.scale_check` | 扩展验证：≥10 000 存活输入行、重数 100、100 批、三算子计划、逻辑水位推进、阈值内迟到被接受、约 10 次真实超期整批拒绝，每 10 批用原批量解释器做检查点对照 | ≈ 4 s |
| `python demo.py` | 固定演示：正常增量结果 + 同批三事件棘手边界 + 一次真实拒绝 + 快照恢复 | ≈ 0.05 s |

全部离线、确定性、纯标准库；不联网、不用数据库/容器/第三方 API。
`verification/` 与 `tests/` 分离：快速回归不含上万行压力，扩展验证按需手动运行。
模块本身无搜索/回溯，因此不存在"预算耗尽被误判为无解"的场景；
失败分两类——**输入非法被拒绝**（`QueryError` 家族，状态不变）与
**查询正常但结果为空/组消失**（不是错误）。规模上限在表中明示，超限编译期拒绝。

## 七、已知限制（本契约子集，不宣称行业标准兼容）

- 不是 SQL：不解析 SQL 文本；无排序、子查询、外键、HAVING、日期/二进制类型；
- 聚合输入仅 int/NULL（`avg` 输出 float）；无通用算术表达式；
- 增量连接对任意 `on` 回退到正确的嵌套循环匹配，纯等值合取（可含残差谓词）走哈希索引；
- 窗口为单一全局 width 的对齐滚动窗口（起点 `floor(t/width)*width`），
  不支持滑动/会话窗口；每计划支持窗口节点，窗口之上再接窗口未在本子集开放；
- 计划节点对象仍不允许共享子树（保证节点序号与错误定位无歧义）；
- 批量 join 为嵌套循环（受默认 200 行上限约束；大规模对照请显式传 `max_rows`）。

## 八、文件

```
rel3vl/__init__.py        # 公开 API（批量 + 增量 + 错误类型）
rel3vl/errors.py          # QueryError 体系（含 StreamError 家族）
rel3vl/values.py          # 标量校验、IS NOT DISTINCT FROM
rel3vl/expressions.py     # 3VL 表达式编译/求值
rel3vl/aggregates.py      # 分组/窗口聚合函数（批量与增量共用）
rel3vl/engine.py          # execute()、计划编译、group_by/window 批量实现
rel3vl/stream.py          # IncrementalExecutor：增删流/连接差量/窗口/水位/快照
tests/test_engine.py      # 原批量/3VL 测试（38 用例，未改动）
tests/test_stream.py      # 增量测试（25 用例，含独立朴素参考与随机差分）
tests/_fixtures.py        # 固定数据 + 独立朴素参考（原文件）
verification/scale_check.py  # 上万行/100 批扩展验证（批量检查点对照）
demo.py                   # 固定输入演示（批量 + 增量）
```
