"""rel3vl 固定输入演示（离线、确定性、纯标准库）。

运行：``python demo.py``（约 1 秒内完成）。

演示内容：
1. 两种过滤结果：普通比较过滤（UNKNOWN 行被丢）与 ``is_null`` 过滤；
2. LEFT JOIN 的 ON 条件与连接后 WHERE 的差异（3VL 经典边界）；
3. 相关存在性子查询：``exists`` / ``not_exists``（含外层重复行与两层嵌套）；
4. 两个真实触发的拒绝：引用不存在的列、引用未绑定的外层来源。

所有数据由本文件内确定性代码生成，无网络、无 SQL、无数据库。
"""

from __future__ import annotations

from rel3vl import QueryError, execute
from rel3vl.errors import SchemaError


# ------------------------------------------------------------ 确定性示例数据

def build_tables():
    # 员工：覆盖普通行、重复部门、NULL 分数、NULL 部门；无随机源。
    employees = {
        "columns": ["id", "dept", "score"],
        "rows": [
            [1, "A", 10],
            [2, "A", None],   # 分数未知
            [3, "B", 20],
            [4, None, 30],    # 部门未知
        ],
    }
    departments = {
        "columns": ["did", "dname"],
        "rows": [
            ["A", "Alpha"],
            ["B", "Beta"],
            [None, "Mystery"],  # NULL 主键：NULL = NULL 为 UNKNOWN，永不匹配
        ],
    }
    return {"employees": employees, "departments": departments}


# ------------------------------------------------------------ 表达式小工具

def col(t, c):
    return {"kind": "col", "table": t, "column": c}


def lit(v):
    return {"kind": "lit", "value": v}


def cmp(op, l, r):
    return {"kind": "cmp", "op": op, "left": l, "right": r}


def band(l, r):
    return {"kind": "and", "left": l, "right": r}


def is_null(e):
    return {"kind": "is_null", "expr": e}


def scan(table, alias):
    return {"op": "scan", "table": table, "as": alias}


def bind(from_source, from_column, as_source, as_column=None):
    return {
        "from": {"table": from_source, "column": from_column},
        "as": as_source,
        "column": as_column if as_column is not None else from_column,
    }


def exists(input_node, subplan, correlate, negate=False):
    return {
        "op": "not_exists" if negate else "exists",
        "input": input_node,
        "subplan": subplan,
        "correlate": correlate,
    }


def print_rows(title, result):
    header = ", ".join(f"{c['source']}.{c['name']}" for c in result["columns"])
    print(f"\n{title}（{len(result['rows'])} 行）")
    print(f"  {header}")
    for row in result["rows"]:
        print(f"  {row}")


def main():
    tables = build_tables()
    emp = scan("employees", "e")

    # --- 过滤结果一：score >= 15 —— NULL 分数产生 UNKNOWN，行被丢弃
    plan_ge = {
        "op": "filter",
        "input": emp,
        "expr": cmp(">=", col("e", "score"), lit(15)),
    }
    print_rows("[过滤一] filter(score >= 15)：仅 TRUE 保留，UNKNOWN(NULL) 丢弃",
               execute(plan_ge, tables))

    # --- 过滤结果二：is_null(score) —— 显式 NULL 判定，不产生 UNKNOWN
    plan_null = {
        "op": "filter",
        "input": scan("employees", "e"),
        "expr": is_null(col("e", "score")),
    }
    print_rows("[过滤二] filter(is_null(score))：直接判定 NULL",
               execute(plan_null, tables))

    # --- LEFT JOIN：ON 附加条件 vs 连接后 WHERE
    on_plan = {
        "op": "left_join",
        "left": scan("employees", "e"),
        "right": scan("departments", "d"),
        "on": band(
            cmp("=", col("e", "dept"), col("d", "did")),
            cmp("=", col("e", "id"), lit(1)),
        ),
    }
    print_rows("[ON] LEFT JOIN ON dept=did AND id=1：未匹配左行补 NULL",
               execute(on_plan, tables))

    basic_left = {
        "op": "left_join",
        "left": scan("employees", "e"),
        "right": scan("departments", "d"),
        "on": cmp("=", col("e", "dept"), col("d", "did")),
    }
    where_plan = {
        "op": "filter",
        "input": basic_left,
        "expr": cmp("=", col("e", "id"), lit(1)),
    }
    print_rows("[WHERE] LEFT JOIN ... 后 WHERE id=1：补 NULL 行也被过滤",
               execute(where_plan, tables))

    # --- 相关存在性：部门真实存在的员工（exists）
    # 子计划按绑定 o.dept 逐外行求值；dept 为 NULL 时比较 UNKNOWN -> 无匹配
    exists_plan = exists(
        scan("employees", "e"),
        {
            "op": "filter",
            "input": scan("departments", "d"),
            "expr": cmp("=", col("d", "did"), col("o", "dept")),
        },
        [bind("e", "dept", "o")],
    )
    print_rows("[EXISTS] 存在同名部门的员工（dept=NULL 的行 UNKNOWN 被丢）",
               execute(exists_plan, tables))

    # --- 相关不存在性：没有任何同名部门的员工（not_exists）
    not_exists_plan = exists(
        scan("employees", "e"),
        exists_plan["subplan"],
        [bind("e", "dept", "o")],
        negate=True,
    )
    print_rows("[NOT EXISTS] 无同名部门的员工（只剩 dept 为 NULL 的 id=4）",
               execute(not_exists_plan, tables))

    # --- 两层相关嵌套：三层同名列 x，靠来源 id 区分
    nest_tables = {
        "t1": {"columns": ["x"], "rows": [[1], [2], [3], [3]]},
        "t2": {"columns": ["x"], "rows": [[2], [3]]},
        "t3": {"columns": ["x"], "rows": [[3]]},
    }
    nested = exists(
        scan("t1", "a"),
        exists(
            {
                "op": "filter",
                "input": scan("t2", "b"),
                "expr": cmp("=", col("b", "x"), col("p", "x")),
            },
            {
                "op": "filter",
                "input": scan("t3", "c"),
                "expr": cmp("=", col("c", "x"), col("q", "x")),
            },
            [bind("p", "x", "q")],
        ),
        [bind("a", "x", "p")],
    )
    print_rows("[两层嵌套] t1 中'在 t2 出现且又在 t3 出现'的 x（重复行保留重数）",
               execute(nested, nest_tables))

    # --- 真实触发的拒绝边界：引用不存在的列
    bad_plan = {
        "op": "filter",
        "input": scan("employees", "e"),
        "expr": cmp("=", col("e", "salary"), lit(0)),  # salary 列不存在
    }
    print("\n[拒绝边界] 引用非法列 e.salary：")
    try:
        execute(bad_plan, tables)
    except SchemaError as exc:
        print(f"  按契约拒绝（SchemaError，是 QueryError 的子类）：\n    {exc}")
    else:  # pragma: no cover - 演示中不应发生
        raise AssertionError("非法列必须被拒绝")

    # 再展示一个相关作用域边界：子计划引用未绑定的外层来源 id
    unbound_plan = exists(
        scan("employees", "e"),
        {
            "op": "filter",
            "input": scan("departments", "d"),
            "expr": cmp("=", col("d", "did"), col("e", "dept")),  # e 未在 correlate 声明
        },
        [],
    )
    print("\n[拒绝边界] 子计划越过声明作用域引用外层来源 e：")
    try:
        execute(unbound_plan, tables)
    except SchemaError as exc:
        print(f"  按契约拒绝（SchemaError）：\n    {exc}")
    else:  # pragma: no cover
        raise AssertionError("未绑定的外层引用必须被拒绝")

    # 再展示一个数值卫生边界：NaN 禁止进入表数据
    bad_tables = {"t": {"columns": ["x"], "rows": [[float("nan")]]}}
    print("\n[拒绝边界] 表数据含 NaN：")
    try:
        execute(scan("t", "t"), bad_tables)
    except QueryError as exc:
        print(f"  按契约拒绝（QueryError）：\n    {exc}")
    else:  # pragma: no cover
        raise AssertionError("NaN 必须被拒绝")

    print("\n演示完成：以上结果均为现场计算，无预录、无 sleep。")


if __name__ == "__main__":
    main()
