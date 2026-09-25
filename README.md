# rel3vl —— 三值逻辑（3VL）下的关系查询解释器

离线、嵌入式、纯标准库的可复用 Python 模块。关系是**带重复行的有限元组袋（bag）**，
谓词采用 SQL 风格三值逻辑（TRUE / FALSE / UNKNOWN），不解析 SQL、不调用任何数据库。

- Python：3.14.7（仅用标准库，3.10+ 语法亦可运行，开发与验收环境为 3.14.7）
- 依赖：**无第三方依赖**；无需网络、账号、密钥、Docker 或外部服务
- 测试：`./.venv/bin/python -m unittest discover -s tests -v`
- 演示：`./.venv/bin/python demo.py`（实测约 0.02 秒，远小于 90 秒预算，现场计算、无 sleep、无预录）

## 接口

```python
from rel3vl import execute, QueryError

result = execute(plan, tables)
# result = {
#   "columns": [{"source": "e", "name": "id"}, ...],
#   "rows":    [[...], ...],          # list[list]，顺序确定
# }
```

所有非法输入抛 `QueryError`（`ValueError` 子类）的子类：
`PlanError` / `SchemaError` / `TableError` / `ExpressionError`。
错误消息带稳定定位前缀：`[plan]`、`[node#<序号>]`、`[node#<序号>/expr]`、`[table:<表名>]`
（含行号、列名）。节点序号是其在计划树深度优先遍历中的出现次序，从 0 开始；
节点也可带字符串 `"id"` 字段，错误消息会一并显示。

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
- `float` 的 NaN / +Infinity / -Infinity **拒绝**；不支持的类型（list/dict 等）**拒绝**，不隐式转换。
- `bool` 是独立类型，不会被静默当作整数；`True` 与 `1` 之间的比较直接报错。

### 计划节点（JSON 风格 dict，判别字段 `op`）

| op | 形态 | 语义 |
|---|---|---|
| `scan` | `{"op":"scan","table":"t","as":"e"}` | 扫描基表；`as` 可省（默认=表名），自连接时必须用不同来源 id |
| `filter` | `{"op":"filter","input":N,"expr":E}` | 仅保留谓词为 **TRUE** 的行；FALSE 与 UNKNOWN 都丢弃 |
| `project` | `{"op":"project","input":N,"columns":[{"name":"v","table":"","expr":E}]}` | 逐表达式投影；`table` 可省（默认 `""`）；输出限定名不得重复 |
| `inner_join` | `{"op":"inner_join","left":N,"right":N,"on":E}` | 嵌套循环；`on` 仅为 TRUE 时匹配 |
| `left_join` | 同上 | 左行对**所有**右行都无 TRUE 匹配时，补一行右侧全 NULL |
| `union_all` | `{"op":"union_all","left":N,"right":N}` | 袋拼接：先左后右；两边列名序列必须一致 |
| `distinct` | `{"op":"distinct","input":N}` | 袋去重，保留每组首次出现，顺序确定 |
| `exists` / `not_exists` | `{"op":"exists","input":N,"subplan":N,"correlations":[绑定,...]}` | **相关存在性过滤**，见下节 |

### 相关 EXISTS / NOT EXISTS（本版新入口）

表达"对这一行，另一个计划里是否有匹配"。对 `input` 的每一行，用该行的相关列值
参数化执行一次 `subplan`：`exists` 保留子计划**至少产生一行**的外层行，
`not_exists` 保留产生**零行**的外层行。

```python
plan = {
    "op": "exists",                       # 或 "not_exists"
    "input": {"op": "scan", "table": "customers", "as": "c"},
    "subplan": {                          # 子计划：任意合法计划树
        "op": "filter",
        "input": {"op": "scan", "table": "orders", "as": "o"},
        "expr": {"kind": "cmp", "op": "=",
                 "left":  {"kind": "col", "table": "o",  "column": "cid"},
                 "right": {"kind": "col", "table": "oc", "column": "cid"}},
    },
    "correlations": [                     # 显式相关列绑定，必填（可为空列表）
        {"outer_table": "c", "outer_column": "cid",
         "inner_alias": "oc", "inner_name": "cid"},
    ],
}
```

语义与限制：

1. **存在性只看行数**：子计划产生全 NULL 行也算存在；行中字段是否为 NULL 无关。
   子计划内部的比较仍走三值逻辑 —— `NULL = NULL` 为 UNKNOWN，不会让内表行通过
   （`is_null` 等严格判定照常工作）。
2. **外层袋原样保留**：输出模式 = `input` 的模式；外层重复行保留重数与顺序；
   内层有多个匹配时外层行**只出现一次**，不做连接式复制。
3. **显式列绑定，词法作用域**：子计划只能通过 `correlations` 声明的
   `(inner_alias, inner_name)` 引用外层值。引用未声明的外层列、不存在的外层列，
   或在孙计划中引用未沿链路透传的列，一律在**编译期**抛 `SchemaError`
   （错误信息同时列出实际可用列与已声明绑定），绝不会静默当作 NULL。
   绑定的内层限定名不得与子计划自身实际列同名（避免引用歧义），重复绑定也被拒绝。
4. **两层相关嵌套**：`exists` 的子计划内可以再有一个 `exists`
   （`rel3vl.MAX_CORRELATION_DEPTH = 2`）。孙计划引用子计划外层输入的列直接绑定；
   引用更外层的值必须在两层的 `correlations` 中**逐次显式透传**（作用域不自动跨层）。
   第三层在编译期拒绝。
5. **相关键缓存**：同一 `exists` 节点的一次执行内，按
   `(inner_alias, inner_name, 值键)` 元组缓存每个外层行的"子计划是否产生行"，
   外层重复行只执行一次子计划。值键复用 DISTINCT 的不可区分口径：
   NULL 用模块级唯一哨兵（不与 `0`/`""`/`"None"` 等任何真实值撞键），
   bool 带类型标签（`True` 与 `1` 不共享），`1` 与 `1.0` 同键；
   来源限定名参与成键，两个不同绑定即便值相同也不会互相误用。
   缓存是 `run` 调用栈上的局部对象，执行结束（含异常向外传播）即回收 ——
   一次失败的执行不会污染下一次 `execute`。
6. **旧接口不变**：`scan/filter/project/inner_join/left_join/union_all/distinct`
   的节点形态、表达式 `kind`、结果格式与错误体系完全保留；
   `correlations` 为空列表时即不相关 EXISTS（子计划非空则全保留）。

### 表达式（判别字段 `kind`）

| kind | 形态 | 说明 |
|---|---|---|
| `lit` | `{"kind":"lit","value":1}` | 字面量，同样拒绝 NaN/Inf |
| `col` | `{"kind":"col","table":"e","column":"id"}` | 来源 id + 列名限定引用；不存在即 `SchemaError` |
| `cmp` | `{"kind":"cmp","op":"=","left":E,"right":E}` | `= <> < <= > >=`；任一操作数为 NULL → UNKNOWN |
| `and` / `or` | `{"kind":"and","left":E,"right":E}` | 3VL：`F AND U = F`、`T OR U = T` 等 |
| `not` | `{"kind":"not","expr":E}` | `NOT UNKNOWN = UNKNOWN` |
| `is_null` | `{"kind":"is_null","expr":E}` | 严格 NULL 判定，返回 TRUE/FALSE |
| `coalesce` | `{"kind":"coalesce","args":[E,...]}` | 返回首个非 NULL，全 NULL 则 NULL |

逻辑运算的操作数必须是 bool 或 NULL；比较不做跨类型隐式转换
（`"3" = 3`、`True = 1` 直接报 `ExpressionError`）；数值允许 int/float 混合比较。

## 输入上限（接口边界）

- 每张输入表最多 **200 行**（`rel3vl.MAX_ROWS_PER_TABLE`）；
- 单棵计划最多 **10 个节点**（`rel3vl.MAX_NODES`，exists/not_exists 节点同样计入）；
- 相关 EXISTS 最多嵌套 **2 层**（`rel3vl.MAX_CORRELATION_DEPTH`）；
- 表达式嵌套最多 200 层（防止病态递归）。

超限在编译期拒绝。**本模块无搜索/回溯类算法，因此不存在"预算耗尽 vs 证明无解"
的区分场景；两类失败分别对应"输入超限被拒绝"与"查询正常执行但结果为空"，
空结果不是错误。**

## 设计取舍

1. **先整树编译、后执行**：计划先做结构遍历（编号、计数、环/共享子树检测），
   再自底向上推导模式并编译表达式；任何错误都在处理第一行之前抛出，
   `execute` 不修改入参，因此失败不会留下部分变更。
2. **袋语义与确定顺序**：关系用 `list[tuple]` 表示；scan 保留表内顺序，
   join 按左行×右行的嵌套顺序输出，union_all 先左后右，distinct 保留首现。
   重复元组按多重数真实保留。
3. **NULL 传播与 3VL**：比较遇 NULL 得 UNKNOWN；`filter` 与连接 `on` 都只认 TRUE；
   `left_join` 只在左行**零匹配**时补一行 NULL（不是每个未匹配右行补一次）。
4. **ON 与连接后 WHERE 不等价**：`LEFT JOIN ... ON c AND p` 中 `p` 失败的左行会补 NULL；
   把 `p` 放到连接后的 WHERE 则补 NULL 行也会被过滤。测试与 demo 均固化该差异。
5. **DISTINCT 的相等是 `IS NOT DISTINCT FROM`**：两个 NULL 去重时视为相同
   （区别于 `NULL = NULL` 为 UNKNOWN）；`1` 与 `1.0` 数学值相同视为同组；
   `True`/`False` 作为独立类型不与 `1`/`0` 合并。
6. **列的身份是 `(source_id, column_name)`**：不同来源允许同名列；
   同限定名冲突（典型：未加别名的自连接）在编译期拒绝。相关绑定同样以
   限定名对内可见，作用域词法且显式，未声明即 `SchemaError`。
7. **相关 EXISTS 的执行模型**：所有节点编译为 `run(frame)`，帧是
   `{参数名: 外层行取值}` 的不可变映射；根节点以空帧启动，进入 exists 节点时
   按 `correlations` 从外层行/上层帧抽值构造子帧。逐外层行执行 + 局部相关键缓存，
   不产生任何跨执行的可变状态。
8. **数值口径**：比较与去重直接使用 Python 机器整数/双精度浮点语义
   （int 为任意精度；float 为 IEEE-754 binary64）。**输入侧已拒绝 NaN/Infinity，
   模块不产生任何新数值（无算术运算），故不存在结果舍入误差，也不设容差**；
   `1` 与 `1.0` 的相等按二进制 64 位双精度 `==` 判定，不使用任何 epsilon。

## 测试

测试（`tests/`）包含全部验收点：

- `test_engine.py`（38 个，原回归）：手工袋计数、ON 与连接后 WHERE 差异、
  `NULL <> NULL`、空右表、重复左行、重复右行乘积、union_all 拼接顺序、
  distinct 的 NULL/类型语义、来源限定与自连接、上限、环检测、数值卫生；
- `test_correlated_exists.py`（29 个，本版新增）：相关 EXISTS/NOT EXISTS 的
  手工袋计数、全 NULL 子计划行、外层重数与顺序、内层多匹配不复制、
  UNKNOWN 比较、空内表、`is_null` 相关参数、未声明/不存在列的编译期拒绝、
  绑定畸形与重名、两层相关嵌套（同名列 + 参数透传）、三层拒绝、
  相关缓存键规则（NULL 哨兵/bool 标签/来源限定名）、失败不污染下次执行、
  旧 project/join/distinct 接口在子计划内回归。

`tests/_fixtures.py` 中另有一份**独立编写**的朴素袋语义参考（逐外层行执行、
Counter + dict 行，代码路径与核心完全不同，且不 import 被测核心），
只用于小样本与小规模穷举的补充交叉校验；验收期望值以测试中的手工袋计数为准，
不做"核心调用核心"式的循环验证。

## 已知限制（本契约子集，不宣称行业标准兼容）

- 不是 SQL：不解析 SQL 文本，无聚合、排序、外键、类型系统、日期/二进制类型；
  相关能力仅 exists/not_exists 过滤节点，无标量子查询、无相关更新/删除；
- 无算术/函数字段，比较符与逻辑算符即全部表达式能力；
- union_all 只按列名序列一致校验，不做隐式类型协调；
- join 为嵌套循环实现（受 200 行上限约束，规模极小，未做哈希/排序优化）；
- 相关 EXISTS 为逐外层行执行（按相关键缓存去重），相关嵌套最多两层，
  绑定只传值、不传递子计划输出列；
- 计划节点对象不允许共享子树（同一 dict 被两个分支引用会被拒绝），以保证"节点序号"
  与错误定位无歧义。

## 文件

```
rel3vl/__init__.py      # 公开 API
rel3vl/errors.py        # QueryError 体系
rel3vl/values.py        # 标量校验、IS NOT DISTINCT FROM、缓存值键 value_key
rel3vl/expressions.py   # 3VL 表达式编译/求值（含内部参数表达式 param）
rel3vl/engine.py        # execute(plan, tables)、计划编译、相关帧与存在性节点
tests/test_engine.py            # 原回归测试（38 个用例）
tests/test_correlated_exists.py # 相关 EXISTS 测试（29 个用例）
tests/_fixtures.py      # 固定数据 + 独立朴素参考（支持相关节点）
demo.py                 # 固定输入演示（含相关 EXISTS）
```
