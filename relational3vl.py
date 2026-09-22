"""relational3vl — 三值逻辑下的关系查询解释器（离线、纯标准库）。

本模块只实现题面约定的有限协议子集：
  * 关系 = 带重复行的有限元组袋（bag），行保序；
  * 计划节点: scan / filter / project / inner_join / left_join /
    union_all / distinct；
  * 表达式: col / lit / eq / ne / lt / le / gt / ge / and / or / not /
    is_null / coalesce；
  * 三值逻辑 (3VL): TRUE / FALSE / UNKNOWN(以 None 表示)。
      任何一侧为 NULL 的比较 -> UNKNOWN；
      filter 与连接谓词只接受“为 TRUE”的行；
      left_join 仅对完全没有 TRUE 匹配的左行补 NULL。

公开接口只有 execute(plan, tables) 与下面的异常类。不解析 SQL、不调用
任何数据库或第三方同类实现。
"""

from __future__ import annotations

import math
from typing import Any

# ---------------------------------------------------------------------------
# 接口边界
# ---------------------------------------------------------------------------

MAX_TABLE_ROWS = 200
"""每张输入表允许的最大行数（200 行，含边界值；201 行即拒绝）。"""

MAX_PLAN_NODES = 10
"""单个计划允许的最大关系节点数（表达式不计入）。"""

MAX_INTERMEDIATE_ROWS = 100_000
"""防御性中间结果行数上限。题面只承诺 200 行/表；该上限让病态笛卡尔
序列快速、可定位地失败，而不是静默耗尽内存。耗尽时抛 QueryLimitError，
与“输入非法”类错误区分。"""


# ---------------------------------------------------------------------------
# 错误语义：所有错误都继承 QueryError，消息带节点/列等定位信息
# ---------------------------------------------------------------------------

class QueryError(Exception):
    """execute() 抛出的所有错误的基类。"""


class PlanError(QueryError):
    """计划/表达式结构非法：未知节点、缺字段、多余字段、算子参数个数错等。"""


class SchemaError(QueryError):
    """名字解析或模式冲突：列不存在、列重名歧义、union 模式不一致等。"""


class DataError(QueryError):
    """数据本身非法：表不存在、行宽不符、取值类型不支持、出现禁止值等。"""


class QueryTypeError(QueryError):
    """运行期类型错误：如字符串与整数比较、filter 谓词不是逻辑值。"""


class QueryLimitError(QueryError):
    """触发接口边界或资源上限（行数、节点数、中间结果上限）。"""


# ---------------------------------------------------------------------------
# 内部常量
# ---------------------------------------------------------------------------

_NODE_KEYS = {
    "scan": {"type", "table"},
    "filter": {"type", "input", "expr"},
    "project": {"type", "input", "columns"},
    "inner_join": {"type", "left", "right", "on"},
    "left_join": {"type", "left", "right", "on"},
    "union_all": {"type", "left", "right"},
    "distinct": {"type", "input"},
}

_CHILDREN = {
    "scan": (),
    "filter": ("input",),
    "project": ("input",),
    "inner_join": ("left", "right"),
    "left_join": ("left", "right"),
    "union_all": ("left", "right"),
    "distinct": ("input",),
}

_EXPR_KEYS = {
    "col": {"op", "source", "name"},
    "lit": {"op", "value"},
    "eq": {"op", "left", "right"},
    "ne": {"op", "left", "right"},
    "lt": {"op", "left", "right"},
    "le": {"op", "left", "right"},
    "gt": {"op", "left", "right"},
    "ge": {"op", "left", "right"},
    "and": {"op", "left", "right"},
    "or": {"op", "left", "right"},
    "not": {"op", "expr"},
    "is_null": {"op", "expr"},
    "coalesce": {"op", "args"},
}

_COMPARISONS = {"eq", "ne", "lt", "le", "gt", "ge"}


# ---------------------------------------------------------------------------
# 取值校验：数值拒绝 NaN/Infinity；整数位拒绝 bool；只支持 int/float/str/None
# ---------------------------------------------------------------------------

def _validate_value(value: Any, loc: str) -> None:
    """输入数据 / lit 字面量的取值域：int、有限 float、str、NULL。

    bool 被拒绝（即使它是 int 子类——“整数参数拒绝 bool”）。
    """
    if value is None:
        return
    # 注意 bool 是 int 的子类，必须先判 bool。
    if isinstance(value, bool):
        raise DataError(
            f"{loc}: 不接受布尔字面量/布尔数据（整数位禁止使用 bool），得到 {value!r}"
        )
    _validate_scalar_body(value, loc)


def _validate_output_value(value: Any, loc: str) -> None:
    """project 输出取值域：数据域 ∪ 计算布尔。

    输入表与 lit 永远不含 bool；bool 只能由比较 / is_null 等逻辑表达式在
    投影中“计算出来”（对应 SQL 的 SELECT x IS NULL）。比较本身仍只在
    数值/字符串数据域上工作，布尔不能混入比较运算元。
    """
    if value is True or value is False:
        return
    if value is None:
        return
    if isinstance(value, bool):  # pragma: no cover - 上面已穷尽 bool 单例
        raise DataError(f"{loc}: 非法布尔值 {value!r}")
    _validate_scalar_body(value, loc)


def _validate_scalar_body(value: Any, loc: str) -> None:
    if isinstance(value, int):
        return
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise DataError(f"{loc}: 数值必须有限，禁止 NaN/Infinity，得到 {value!r}")
        return
    if isinstance(value, str):
        return
    raise DataError(
        f"{loc}: 不支持的值类型 {type(value).__name__}；"
        "仅允许 int、有限 float、str 与 NULL(None)"
    )


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


# ---------------------------------------------------------------------------
# 三值逻辑真值表：TRUE=True, FALSE=False, UNKNOWN=None
# ---------------------------------------------------------------------------

def _vl_and(a: Any, b: Any) -> Any:
    # FALSE 吸收一切；否则任一 UNKNOWN -> UNKNOWN；否则 TRUE。
    if a is False or b is False:
        return False
    if a is None or b is None:
        return None
    return True


def _vl_or(a: Any, b: Any) -> Any:
    # TRUE 吸收一切；否则任一 UNKNOWN -> UNKNOWN；否则 FALSE。
    if a is True or b is True:
        return True
    if a is None or b is None:
        return None
    return False


def _vl_not(a: Any) -> Any:
    if a is None:
        return None
    return not a


def _as_logical(v: Any, loc: str) -> Any:
    # 必须用 is：1 == True、0 == False，不能让整数混入逻辑值。
    if v is True or v is False or v is None:
        return v
    raise QueryTypeError(
        f"{loc}: 谓词必须是逻辑值 TRUE/FALSE/UNKNOWN，"
        f"却得到 {type(v).__name__}: {v!r}"
    )


# ---------------------------------------------------------------------------
# 表（输入）校验：不修改调用方对象，校验后复制行
# ---------------------------------------------------------------------------

def _validate_table(name: Any, table: Any) -> tuple:
    """返回 (schema_tuple, rows_list_of_lists)。"""
    loc = f"table {name!r}"
    if not isinstance(name, str) or not name:
        raise DataError(f"表名必须是非空字符串，得到 {name!r}")
    if not isinstance(table, dict):
        raise DataError(f"{loc}: 表必须是对象 {{'columns', 'rows'}}，得到 {type(table).__name__}")
    if set(table) != {"columns", "rows"}:
        raise PlanError(
            f"{loc}: 表只能含键 'columns' 与 'rows'，实际键 {sorted(table)}"
        )

    raw_cols = table["columns"]
    if not isinstance(raw_cols, (list, tuple)) or not raw_cols:
        raise DataError(f"{loc}: columns 必须是非空列表，得到 {raw_cols!r}")

    schema: list[tuple] = []
    for i, col in enumerate(raw_cols):
        if not isinstance(col, (list, tuple)) or len(col) != 2:
            raise DataError(
                f"{loc}: columns[{i}] 必须是 [来源ID, 列名] 二元组，得到 {col!r}"
            )
        source, cname = col
        if not isinstance(source, str) or not source:
            raise DataError(f"{loc}: columns[{i}] 的来源ID必须是非空字符串，得到 {source!r}")
        if not isinstance(cname, str) or not cname:
            raise DataError(f"{loc}: columns[{i}] 的列名必须是非空字符串，得到 {cname!r}")
        pair = (source, cname)
        if pair in schema:
            raise SchemaError(f"{loc}: 列 {source}.{cname} 在同一表中重复定义，存在歧义")
        schema.append(pair)

    raw_rows = table["rows"]
    if not isinstance(raw_rows, (list, tuple)):
        raise DataError(f"{loc}: rows 必须是列表，得到 {type(raw_rows).__name__}")
    if len(raw_rows) > MAX_TABLE_ROWS:
        raise QueryLimitError(
            f"{loc}: 输入表有 {len(raw_rows)} 行，超过每表最多 {MAX_TABLE_ROWS} 行"
        )

    width = len(schema)
    rows: list[list] = []
    for i, row in enumerate(raw_rows):
        rloc = f"{loc} row[{i}]"
        if not isinstance(row, (list, tuple)):
            raise DataError(f"{rloc}: 行必须是列表/元组，得到 {type(row).__name__}")
        if len(row) != width:
            raise DataError(f"{rloc}: 行宽 {len(row)} 与列数 {width} 不一致")
        new_row = []
        for j, v in enumerate(row):
            _validate_value(v, f"{rloc} column[{j}] ({schema[j][0]}.{schema[j][1]})")
            new_row.append(v)
        rows.append(new_row)

    return tuple(schema), rows


# ---------------------------------------------------------------------------
# 表达式编译：结构全量预检 + 名字解析，返回“行 -> 值”的闭包
# ---------------------------------------------------------------------------

def _compile_expr(expr: Any, schema: tuple, loc: str):
    if not isinstance(expr, dict):
        raise PlanError(f"{loc}: 表达式必须是对象(dict)，得到 {type(expr).__name__}")

    op = expr.get("op")
    if not isinstance(op, str):
        raise PlanError(f"{loc}: 表达式缺少字符串类型的 'op'，得到 {op!r}")
    allowed = _EXPR_KEYS.get(op)
    if allowed is None:
        raise PlanError(f"{loc}: 未知表达式算子 {op!r}")
    if set(expr) != allowed:
        raise PlanError(
            f"{loc}: 算子 {op!r} 只允许键 {sorted(allowed)}，出现多余/缺失键 {sorted(expr)}"
        )

    if op == "col":
        source, cname = expr["source"], expr["name"]
        if not isinstance(source, str) or not source:
            raise PlanError(f"{loc}: col.source 必须是非空字符串")
        if not isinstance(cname, str) or not cname:
            raise PlanError(f"{loc}: col.name 必须是非空字符串")
        pair = (source, cname)
        try:
            idx = schema.index(pair)
        except ValueError:
            available = ", ".join(f"{s}.{n}" for s, n in schema) or "<无列>"
            raise SchemaError(
                f"{loc}: 列 {source}.{cname} 不存在；当前模式可用列: {available}"
            ) from None
        return lambda row: row[idx]

    if op == "lit":
        value = expr["value"]
        _validate_value(value, f"{loc}(lit)")
        return lambda row: value

    if op in _COMPARISONS:
        lf = _compile_expr(expr["left"], schema, f"{loc}.left")
        rf = _compile_expr(expr["right"], schema, f"{loc}.right")

        def compare(row, _op=op, _lf=lf, _rf=rf, _loc=loc):
            a, b = _lf(row), _rf(row)
            if a is None or b is None:
                return None  # NULL 比较 -> UNKNOWN
            num_a, num_b = _is_number(a), _is_number(b)
            if num_a and num_b:
                pass
            elif isinstance(a, str) and isinstance(b, str):
                pass
            else:
                raise QueryTypeError(
                    f"{_loc}: 不可比较的类型 {type(a).__name__} 与 {type(b).__name__} "
                    f"（只有 NULL 产生 UNKNOWN，跨类型比较直接报错）"
                )
            if _op == "eq":
                return a == b
            if _op == "ne":
                return a != b
            if _op == "lt":
                return a < b
            if _op == "le":
                return a <= b
            if _op == "gt":
                return a > b
            return a >= b  # ge

        return compare

    if op == "and":
        lf = _compile_expr(expr["left"], schema, f"{loc}.left")
        rf = _compile_expr(expr["right"], schema, f"{loc}.right")

        def ev_and(row, _lf=lf, _rf=rf, _loc=loc):
            return _vl_and(
                _as_logical(_lf(row), _loc + ".left"),
                _as_logical(_rf(row), _loc + ".right"),
            )

        return ev_and

    if op == "or":
        lf = _compile_expr(expr["left"], schema, f"{loc}.left")
        rf = _compile_expr(expr["right"], schema, f"{loc}.right")

        def ev_or(row, _lf=lf, _rf=rf, _loc=loc):
            return _vl_or(
                _as_logical(_lf(row), _loc + ".left"),
                _as_logical(_rf(row), _loc + ".right"),
            )

        return ev_or

    if op in ("not", "is_null"):
        sub = _compile_expr(expr["expr"], schema, f"{loc}.expr")
        if op == "not":
            return lambda row, _s=sub, _loc=loc: _vl_not(
                _as_logical(_s(row), _loc + ".expr")
            )
        return lambda row, _s=sub: _s(row) is None

    # coalesce
    args = expr["args"]
    if not isinstance(args, list) or not args:
        raise PlanError(f"{loc}: coalesce.args 必须是非空列表")
    funcs = [
        _compile_expr(a, schema, f"{loc}.args[{i}]") for i, a in enumerate(args)
    ]

    def ev_coalesce(row, _funcs=funcs):
        for f in _funcs:
            v = f(row)
            if v is not None:
                return v
        return None

    return ev_coalesce


# ---------------------------------------------------------------------------
# 计划预检：节点计数（上限 10）与结构遍历
# ---------------------------------------------------------------------------

def _count_nodes(node: Any, loc: str = "plan") -> int:
    if not isinstance(node, dict):
        raise PlanError(f"{loc}: 计划节点必须是对象(dict)，得到 {type(node).__name__}")
    ntype = node.get("type")
    if not isinstance(ntype, str):
        raise PlanError(f"{loc}: 节点缺少字符串类型的 'type'，得到 {ntype!r}")
    if ntype not in _NODE_KEYS:
        raise PlanError(f"{loc}: 未知计划节点类型 {ntype!r}")
    if set(node) != _NODE_KEYS[ntype]:
        raise PlanError(
            f"{loc}: 节点 {ntype!r} 只允许键 {sorted(_NODE_KEYS[ntype])}，"
            f"实际键 {sorted(node)}"
        )
    total = 1
    for key in _CHILDREN[ntype]:
        role = {"input": "input", "left": "left", "right": "right"}[key]
        total += _count_nodes(node[key], f"{loc}.{role}({ntype})")
    if total > MAX_PLAN_NODES:
        raise QueryLimitError(
            f"{loc}: 计划节点数达到 {total}，超过最多 {MAX_PLAN_NODES} 个关系节点"
        )
    return total


# ---------------------------------------------------------------------------
# 节点编译：返回 (输出模式, 无参 runner -> list[list])
# ---------------------------------------------------------------------------

def _cap(rows: list, loc: str) -> list:
    if len(rows) > MAX_INTERMEDIATE_ROWS:
        raise QueryLimitError(
            f"{loc}: 中间结果 {len(rows)} 行，超过防御性上限 "
            f"{MAX_INTERMEDIATE_ROWS} 行（题面只承诺每输入表 {MAX_TABLE_ROWS} 行）"
        )
    return rows


def _compile_node(node: dict, role: str, idx: list, tables: dict, table_cache: dict):
    ntype = node["type"]
    seq = idx[0]
    idx[0] += 1
    loc = f"node#{seq}({role}:{ntype})"

    if ntype == "scan":
        tname = node["table"]
        if not isinstance(tname, str) or not tname:
            raise PlanError(f"{loc}: scan.table 必须是非空字符串")
        if tname not in table_cache:
            if tname not in tables:
                raise DataError(f"{loc}: scan 引用了不存在的表 {tname!r}")
            table_cache[tname] = _validate_table(tname, tables[tname])
        schema, src_rows = table_cache[tname]

        def run_scan(_rows=src_rows):
            return [list(r) for r in _rows]

        return schema, run_scan

    if ntype == "filter":
        sub_schema, sub_run = _compile_node(
            node["input"], "input", idx, tables, table_cache
        )
        pred = _compile_expr(node["expr"], sub_schema, loc + " filter-expr")

        def run_filter(_sub=sub_run, _pred=pred, _loc=loc):
            out = []
            for i, row in enumerate(_sub()):
                try:
                    v = _as_logical(_pred(row), _loc)
                except QueryError as ex:
                    ex.add_note(f"位置: {_loc} 的第 {i} 行")
                    raise
                if v is True:  # FALSE 与 UNKNOWN 都丢弃
                    out.append(row)
            return _cap(out, _loc)

        return sub_schema, run_filter

    if ntype == "project":
        sub_schema, sub_run = _compile_node(
            node["input"], "input", idx, tables, table_cache
        )
        cols = node["columns"]
        if not isinstance(cols, list) or not cols:
            raise PlanError(f"{loc}: project.columns 必须是非空列表")
        out_schema: list[tuple] = []
        comps: list = []
        for i, c in enumerate(cols):
            cloc = f"{loc} columns[{i}]"
            if not isinstance(c, dict) or set(c) != {"source", "name", "expr"}:
                raise PlanError(
                    f"{cloc}: 投影项必须是 {{'source','name','expr'}}，得到 {c!r}"
                )
            source, cname = c["source"], c["name"]
            if not isinstance(source, str) or not source:
                raise PlanError(f"{cloc}: source 必须是非空字符串")
            if not isinstance(cname, str) or not cname:
                raise PlanError(f"{cloc}: name 必须是非空字符串")
            pair = (source, cname)
            if pair in out_schema:
                raise SchemaError(f"{cloc}: 投影输出列 {source}.{cname} 重复，存在歧义")
            comp = _compile_expr(c["expr"], sub_schema, cloc + " expr")
            out_schema.append(pair)
            comps.append(comp)
        out_schema_t = tuple(out_schema)

        def run_project(_sub=sub_run, _comps=comps, _oschema=out_schema, _loc=loc):
            out = []
            for i, row in enumerate(_sub()):
                new_row = []
                for j, f in enumerate(_comps):
                    try:
                        v = f(row)
                        # 输出允许数据域 + 逻辑表达式计算出的布尔；
                        # 输入数据与 lit 中的 bool 仍在入口被拒绝。
                        _validate_output_value(
                            v, f"{_loc} 输出列 {_oschema[j][0]}.{_oschema[j][1]}"
                        )
                    except QueryError as ex:
                        ex.add_note(f"位置: {_loc} 的第 {i} 行")
                        raise
                    new_row.append(v)
                out.append(new_row)
            return _cap(out, _loc)

        return out_schema_t, run_project

    if ntype in ("inner_join", "left_join"):
        l_schema, l_run = _compile_node(
            node["left"], "left", idx, tables, table_cache
        )
        r_schema, r_run = _compile_node(
            node["right"], "right", idx, tables, table_cache
        )
        overlap = set(l_schema) & set(r_schema)
        if overlap:
            desc = ", ".join(f"{s}.{n}" for s, n in sorted(overlap))
            raise SchemaError(
                f"{loc}: 连接两侧出现相同的全限定列 {desc}，请用不同来源ID消歧"
            )
        joined_schema = tuple(l_schema) + tuple(r_schema)
        pred = _compile_expr(node["on"], joined_schema, loc + " on-expr")
        width_r = len(r_schema)
        is_left = ntype == "left_join"

        def run_join(_l=l_run, _r=r_run, _pred=pred, _wr=width_r,
                     _left=is_left, _loc=loc):
            left_rows, right_rows = _l(), _r()
            out: list[list] = []
            for li, lrow in enumerate(left_rows):
                matched = False
                for ri, rrow in enumerate(right_rows):
                    combo = lrow + rrow
                    try:
                        v = _as_logical(_pred(combo), _loc)
                    except QueryError as ex:
                        ex.add_note(f"位置: {_loc}，左行 {li} 右行 {ri}")
                        raise
                    if v is True:  # 连接谓词：仅 TRUE 匹配
                        matched = True
                        out.append(combo)
                        # 即时预算检查：不在循环结束后才发现超限
                        if len(out) > MAX_INTERMEDIATE_ROWS:
                            raise QueryLimitError(
                                f"{_loc}: 中间结果超过防御性上限 "
                                f"{MAX_INTERMEDIATE_ROWS} 行（题面只承诺每输入表 "
                                f"{MAX_TABLE_ROWS} 行；已在左行 {li} 右行 {ri} 处中止）"
                            )
                if _left and not matched:
                    # left_join：整行在右表完全无 TRUE 匹配时才补 NULL
                    out.append(list(lrow) + [None] * _wr)
            return out

        return joined_schema, run_join

    if ntype == "union_all":
        l_schema, l_run = _compile_node(
            node["left"], "left", idx, tables, table_cache
        )
        r_schema, r_run = _compile_node(
            node["right"], "right", idx, tables, table_cache
        )
        if l_schema != r_schema:
            raise SchemaError(
                f"{loc}: union_all 要求两侧模式按位置完全一致（来源ID与列名）；"
                f"左={[list(c) for c in l_schema]} 右={[list(c) for c in r_schema]}"
            )

        def run_union(_l=l_run, _r=r_run, _loc=loc):
            return _cap(_l() + _r(), _loc)  # 先行保序：先左后右

        return l_schema, run_union

    # distinct
    sub_schema, sub_run = _compile_node(
        node["input"], "input", idx, tables, table_cache
    )

    def run_distinct(_sub=sub_run, _loc=loc):
        seen: set = set()
        out: list[list] = []
        for row in _sub():  # 首次出现顺序，保证确定性
            key = tuple(row)
            if key not in seen:
                seen.add(key)
                out.append(row)
        return _cap(out, _loc)

    return sub_schema, run_distinct


# ---------------------------------------------------------------------------
# 公开入口
# ---------------------------------------------------------------------------

def execute(plan: dict, tables: dict) -> dict:
    """对逻辑查询计划做袋语义解释执行。

    plan  : 计划节点（dict 树），见模块文档与 README。
    tables: {表名: {"columns": [[来源ID, 列名], ...], "rows": [[...], ...]}}。

    返回  : {"columns": [[来源ID, 列名], ...], "rows": [[...], ...]}，
            行按文档约定的确定顺序排列；返回对象为全新构造，不别名调用方数据。

    计划在处理任何一行之前完成全部结构/模式/取值预检；本函数纯函数式，
    不会修改 plan 与 tables，因此失败时不存在部分变更。
    """
    if not isinstance(plan, dict):
        raise PlanError(f"plan 必须是对象(dict)，得到 {type(plan).__name__}")
    if not isinstance(tables, dict):
        raise DataError(f"tables 必须是对象(dict)，得到 {type(tables).__name__}")

    _count_nodes(plan, "plan")  # 节点数与结构预检（含 10 节点上限）
    table_cache: dict = {}
    idx = [0]
    schema, runner = _compile_node(plan, "root", idx, tables, table_cache)
    rows = runner()
    return {"columns": [list(c) for c in schema], "rows": rows}
