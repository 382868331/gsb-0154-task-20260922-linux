# relational3vl — 三值逻辑下的关系查询解释模块

离线、可导入的嵌入式查询题库：对**逻辑查询计划**（dict 树，不是 SQL）做袋语义
（bag semantics，允许重复行）解释执行，比较与连接谓词遵循 SQL 风格的
**三值逻辑 (3VL)**：`TRUE / FALSE / UNKNOWN`，`NULL` 参与比较一律为
`UNKNOWN`；`filter` 与连接谓词只保留为 `TRUE` 的行；`LEFT JOIN` 仅对
**完全没有 TRUE 匹配**的左行补 `NULL`。纯 Python 标准库实现，不解析 SQL、
不调用任何数据库或第三方同类实现。

## 运行环境

- Python **3.14.7**（仅使用自 3.x 起稳定的标准库特性，3.10+ 亦可运行）
- Linux 原生；**无需安装任何第三方依赖**，无需网络 / 账号 / 密钥 / Docker /
  数据库 / 中间件。
- 核心、测试、演示全部离线，示例数据由仓库内确定性代码构造。

## 准确命令

```bash
# 必要测试（31 个用例）
python -m unittest discover -s tests -v

# 固定输入演示：两种过滤结果 + LEFT JOIN 的 ON/WHERE 差异 + 一个真实触发的异常
python demo.py
```

演示为真实计算（无 `sleep`、无预录结果），在本机约 0.02 秒完成（契约预算约 8 秒）。

## 公开接口

```python
from relational3vl import execute
result = execute(plan, tables)
# result = {"columns": [[来源ID, 列名], ...], "rows": [[...], ...]}
```

- `tables: dict[str, Table]`，`Table = {"columns": list[[source_id, name]],
  "rows": list[list]}`。列名是 **(来源ID, 列名)** 二元组，来源ID 用于消除
  重名歧义（如连接两侧都有 `id`）。
- 单元格取值域（输入表）：`int`、有限 `float`、`str`、`NULL`（Python `None`）。
  查询结果的数据列同域；`project` 的逻辑表达式额外可产出计算布尔（见下）。
  - 拒绝 `NaN` / `+Infinity` / `-Infinity`（输入数据与 `lit` 字面量均拒绝）。
  - 拒绝 `bool`（即使 `bool` 是 `int` 子类；整数位不得用 `True/False`）。
  - 不支持 list/dict/datetime 等其他类型。
- 返回对象是全新构造的；`execute` 是纯函数，不修改 `plan`/`tables`，
  因此失败时不存在部分变更。
- 结果顺序确定（见“排序约定”）。

### 计划节点（7 种）

| 节点 | 字段 | 语义 |
|---|---|---|
| `scan` | `table` | 扫描命名输入表，原序复制 |
| `filter` | `input`, `expr` | 仅保留谓词为 `TRUE` 的行（`FALSE`/`UNKNOWN` 都丢弃） |
| `project` | `input`, `columns` | `columns` 为 `{source,name,expr}` 列表；按序产出重命名/重排的列 |
| `inner_join` | `left`, `right`, `on` | `on` 为 `TRUE` 才匹配；重复行按袋相乘 |
| `left_join` | `left`, `right`, `on` | 同上；左行在整棵右表上无任何 `TRUE` 匹配时补一行右侧全 `NULL` |
| `union_all` | `left`, `right` | 袋并集（保留重复），要求两侧模式按位置完全一致 |
| `distinct` | `input` | 按整行去重，保留各行首次出现；`NULL` 与 `NULL` 视为相同 |

### 表达式（13 个算子）

`col(source,name)`、`lit(value)`；比较 `eq/ne/lt/le/gt/ge`；
逻辑 `and/or/not`；`is_null(expr)`；`coalesce(args…)`（非空参数列表，
返回第一个非 NULL 值，全 NULL 则为 NULL）。

- 任一侧为 `NULL` 的比较结果是 `UNKNOWN`，**不是** `FALSE`：
  因此 `NULL = NULL` 与 `NULL <> NULL` 都是 `UNKNOWN`。
- 3VL 真值表：`AND` 中 `FALSE` 吸收一切；`OR` 中 `TRUE` 吸收一切；
  `NOT UNKNOWN = UNKNOWN`。`AND/OR` **不做短路**：两侧都按先左后右
  求值（任一侧为类型错误等仍会抛出，与多数 SQL 引擎一致）。
- 比较只允许 数值↔数值 或 字符串↔字符串；跨类型（如 `1 = "a"`）
  直接抛 `QueryTypeError`，不会被隐式转成 `FALSE`。字符串比较为 Python
  Unicode 码点序。
- 比较/逻辑/`is_null` 的结果属逻辑层，可用于谓词，也可作为 `project`
  的输出列（对应 SQL 的 `SELECT a IS NULL, a = 1`：投影结果允许出现
  **计算出的** 布尔）；但输入表与 `lit` 字面量中永远不允许布尔，
  比较运算元也只能是数值/字符串数据域（`lit(True)` 抛 `DataError`）。

### 排序约定（确定性）

- `scan`：输入行原序。
- `filter` / `project` / `distinct`：保持输入顺序；`distinct` 保留首次出现。
- 连接：外层按左表行序、内层按右表行序；一个左行的所有匹配连续输出，
  补 NULL 行（仅 left join）排在该左行匹配之后。
- `union_all`：先左后右。

### 错误语义

所有错误继承 `relational3vl.QueryError`，消息带节点编号（编译顺序
`node#0..`）、角色（root/left/right/input）与列名，便于定位：

| 异常 | 触发 |
|---|---|
| `PlanError` | 未知节点/算子、缺字段或多余字段、参数形状错 |
| `SchemaError` | 列不存在（消息附全部可用列）、同表/投影/连接侧全限定列重名、`union_all` 模式不一致 |
| `DataError` | 表不存在、行宽不符、NaN/Infinity、输入或 `lit` 中的 bool、不支持类型、投影出不支持类型 |
| `QueryTypeError` | 跨类型比较、谓词位置得到非逻辑值 |
| `QueryLimitError` | 触发接口/资源上限 |

全部结构、模式与字面量预检在处理第一行之前完成；运行期错误（如某行跨类型
比较）会附带出错行位置。空结果是合法语义，与资源类失败明确分开。

## 输入上限

- 每张输入表最多 **200 行**（200 合法，201 拒绝）；列数未单列上限，
  受 10 节点计划规模约束。
- 单个计划最多 **10 个关系节点**（表达式不计入；10 合法，11 拒绝）。
- 另有防御性中间结果上限 **100 000 行**：题面只承诺 200 行/表，该上限使
  病态笛卡尔序列以 `QueryLimitError` 快速失败而不是耗尽内存。
  “搜索/资源预算耗尽”（`QueryLimitError`）与“语义上无解/空结果”严格区分。

## 误差口径（浮点）

- 本模块**不做任何算术运算**（无 `+ - * /`、无聚合、无舍入），浮点只参与
  原样透传与比较，因此**不存在累积误差或近似容差**问题。
- 比较使用 Python 原生 IEEE-754 双精运算：`eq` 即原生 `==`
  （因此 `-0.0 == 0.0` 为真；`NaN` 在输入端即被拒绝，永不进入比较，
  不存在 NaN 自比较问题）；`lt/le/gt/ge` 为原生次序。不使用任何
  epsilon/宽容差，边界值按确切值判定。

## 设计取舍

1. **计划即 JSON 形状的 dict 树**：节点/表达式只允许白名单键，多余键报错，
   避免静默忽略拼写错误；不做任何字符串 DSL 解析。
2. **先全量预检、后逐行执行**：编译期解析列、校验模式与字面量，运行期闭包
   只做取值与 3VL 求值；纯函数式，无批中部分变更。
3. **列用 (来源ID, 名)** 而非位置或短名：连接两侧可同名列而不歧义；
   连接输出模式为左模式接右模式；连接侧全限定列重名直接报错（不做自动加后缀）。
4. **嵌套循环连接、袋相乘、确定顺序**：上限规模（200 行、10 节点）下足够，
   且顺序即规范，不依赖哈希迭代的偶然性。
5. **严格类型**：不做跨类型隐式比较、不把 UNKNOWN 当 FALSE、不把 bool 当 int、
   不把 NaN/Infinity 当合法数值——非法输入给可定位错误，不静默纠正。
6. **独立正确性依据**：测试期望值来自手工袋计数与测试文件内独立实现的
   dict 行参考解释器（与核心代码路径不同、不 import 核心产生期望值）；
   往返一致性（如 project 同构、distinct∘union_all）仅作补充断言。

## 已知限制 / 非目标

- 只实现题面声明的有限子集：没有 SQL 解析、聚合、排序、外连接
  （right/full）、交叉连接语法、子查询、相关变量、类型转换、日期/二进制类型。
- 不声称兼容任何完整行业标准（ANSI SQL/特定数据库方言）；字符串排序是
  Unicode 码点序而非任何 collation。
- `union_all` 要求两侧 (来源ID,列名) 完全按位置一致（比 SQL 的按位置规则
  更严格，属本题契约的有意选择）。
- 嵌套循环复杂度 O(左×右/节点)，仅在公布上限内提供确定、可复核的行为，
  不是大数据引擎。

## 仓库内容

```
relational3vl.py          # 核心：execute(plan, tables) 与错误类型
demo.py                   # 固定输入演示（正常结果 + 实际触发的异常边界）
tests/test_relational3vl.py  # 31 个必要测试 + 独立参考解释器
README.md
```
