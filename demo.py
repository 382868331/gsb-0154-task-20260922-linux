"""rel3vl 固定输入演示（离线、确定性、纯标准库）。

运行：``./.venv/bin/python demo.py``（1 秒内完成）。

演示内容：
1. 两种过滤结果：普通比较过滤（UNKNOWN 行被丢）与 ``is_null`` 过滤；
2. LEFT JOIN 的 ON 条件与连接后 WHERE 的差异（3VL 经典边界）；
3. **相关 EXISTS / NOT EXISTS**：逐外层行判断子计划是否产生行，
   覆盖全 NULL 子计划行、外层重复、UNKNOWN 比较、空内表；
4. 两个真实触发的拒绝：引用不存在的列、子计划引用未声明的外层相关列。

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
    # 相关 EXISTS 数据：客户含重复行与 NULL 客户号；订单含一客户两单与 NULL 单。
    customers = {
        "columns": ["cid", "cname"],
        "rows": [
            [1, "Ann"],
            [1, "Ann"],   # 外层重复行
            [2, "Bob"],
            [3, "Cyd"],   # 无订单
            [None, "Nemo"],  # cid NULL：与 NULL 订单比较为 UNKNOWN
        ],
    }
    orders = {
        "columns": ["oid", "cid"],
        "rows": [
            [10, 1],
            [11, 1],     # 同一客户两个匹配：外层行不复制
            [12, 2],
            [13, None],  # 订单客户未知
        ],
    }
    return {
        "employees": employees,
        "departments": departments,
        "customers": customers,
        "orders": orders,
    }


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


def corr(outer_table, outer_column, inner_alias, inner_name):
    return {
        "outer_table": outer_table,
        "outer_column": outer_column,
        "inner_alias": inner_alias,
        "inner_name": inner_name,
    }


def exists_node(input_node, subplan, correlations, negate=False):
    return {
        "op": "not_exists" if negate else "exists",
        "input": input_node,
        "subplan": subplan,
        "correlations": correlations,
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

    # --- 相关 EXISTS：有订单的客户
    exists_plan = exists_node(
        scan("customers", "c"),
        {
            "op": "filter",
            "input": scan("orders", "o"),
            "expr": cmp("=", col("o", "cid"), col("oc", "cid")),
        },
        [corr("c", "cid", "oc", "cid")],
    )
    print_rows(
        "[EXISTS] customers WHERE EXISTS(orders WHERE o.cid = c.cid)："
        "Ann 两行重数保留；两笔订单不复制外层行；NULL 客户因 UNKNOWN 被排除",
        execute(exists_plan, tables),
    )

    # --- 相关 NOT EXISTS：没有订单的客户
    not_exists_plan = exists_node(
        scan("customers", "c"),
        {
            "op": "filter",
            "input": scan("orders", "o"),
            "expr": cmp("=", col("o", "cid"), col("oc", "cid")),
        },
        [corr("c", "cid", "oc", "cid")],
        negate=True,
    )
    print_rows("[NOT EXISTS] 没有订单的客户（含 UNKNOWN 归入不存在）",
               execute(not_exists_plan, tables))

    # --- 全 NULL 子计划行也算存在：存在性只看行数
    all_null_subplan = {
        "op": "project",
        "input": scan("orders", "o"),
        "columns": [{"name": "z", "expr": lit(None)}],
    }
    null_rows_plan = exists_node(
        scan("customers", "c"), all_null_subplan,
        [corr("c", "cid", "oc", "cid")],
    )
    print_rows("[全NULL行] 子计划只产生全 NULL 投影行：orders 非空 -> EXISTS 恒真",
               execute(null_rows_plan, tables))

    # --- 空内表：EXISTS 全空，NOT EXISTS 原样保留外层
    empty_tables = dict(tables)
    empty_tables["orders"] = {"columns": ["oid", "cid"], "rows": []}
    print_rows("[空内表] EXISTS 对空内表 -> 0 行",
               execute(exists_plan, empty_tables))
    print_rows("[空内表] NOT EXISTS 对空内表 -> 外层 5 行原样保留",
               execute(not_exists_plan, empty_tables))

    # --- 真实触发的拒绝边界：引用不存在的列
    bad_plan = {
        "op": "filter",
        "input": scan("employees", "e"),
        "expr": cmp("=", col("e", "salary"), lit(0)),  # salary 列不存在
    }
    print("\n[拒绝边界一] 引用非法列 e.salary：")
    try:
        execute(bad_plan, tables)
    except SchemaError as exc:
        print(f"  按契约拒绝（SchemaError，是 QueryError 的子类）：\n    {exc}")
    else:  # pragma: no cover - 演示中不应发生
        raise AssertionError("非法列必须被拒绝")

    # --- 拒绝边界二：子计划引用未在 correlations 声明的外层列（不当 NULL）
    bad_corr_plan = exists_node(
        scan("customers", "c"),
        {
            "op": "filter",
            "input": scan("orders", "o"),
            # 直接写外层真实限定名 c.cid，但 correlations 为空：
            "expr": cmp("=", col("o", "cid"), col("c", "cid")),
        },
        [],
    )
    print("\n[拒绝边界二] 子计划引用未声明的外层相关列 c.cid（不得静默当 NULL）：")
    try:
        execute(bad_corr_plan, tables)
    except SchemaError as exc:
        print(f"  按契约拒绝（SchemaError）：\n    {exc}")
    else:  # pragma: no cover
        raise AssertionError("未声明的相关列必须被拒绝")

    # 再展示一个数值卫生边界：NaN 禁止进入表数据
    bad_tables = {"t": {"columns": ["x"], "rows": [[float("nan")]]}}
    print("\n[拒绝边界三] 表数据含 NaN：")
    try:
        execute(scan("t", "t"), bad_tables)
    except QueryError as exc:
        print(f"  按契约拒绝（QueryError）：\n    {exc}")
    else:  # pragma: no cover
        raise AssertionError("NaN 必须被拒绝")

    print("\n演示完成：以上结果均为现场计算，无预录、无 sleep。")


if __name__ == "__main__":
    main()
