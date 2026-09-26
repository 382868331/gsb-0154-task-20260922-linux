"""rel3vl grouping sets 增量演示（离线、确定性、纯标准库）。

运行：``python demo_iteration.py``（实测远低于 90 秒预算，现场计算）。

演示内容：
1. 两级小汇总：filter + inner_join 之后做 grouping sets（按部门名 + 全表），
   手算可核对；grouping 位图区分"真实 NULL"与"汇总产生的空列"；
2. 空表边界：空输入 + 空分组集仍有一行 count=0、sum=NULL；
   空输入 + 非空分组集返回零行；
3. 重复分组集：同一分组集出现两次，结果各保留一份（不去重）；
4. 一个真实触发的拒绝：sum 作用在 bool 列上 -> ExpressionError。

所有数据由本文件内确定性代码生成，无网络、无账号、无密钥、无 sleep、
无预录结果、不接数据库服务。
"""

from __future__ import annotations

from rel3vl import ExpressionError, execute


# ------------------------------------------------------------ 确定性示例数据

def build_tables():
    employees = {
        "columns": ["id", "dept", "score"],
        "rows": [
            [1, "A", 10],
            [2, "A", None],   # 分数未知（NULL）
            [3, "B", 20],
            [4, None, 30],    # 部门未知（NULL）
            [1, "A", 10],     # 袋语义：重复行参与聚合
        ],
    }
    departments = {
        "columns": ["did", "dname"],
        "rows": [["A", "Alpha"], ["B", "Beta"]],
    }
    return {"employees": employees, "departments": departments}


# ------------------------------------------------------------ 计划构造小工具

def col(t, c):
    return {"kind": "col", "table": t, "column": c}


def lit(v):
    return {"kind": "lit", "value": v}


def cmp(op, l, r):
    return {"kind": "cmp", "op": op, "left": l, "right": r}


def scan(table, alias):
    return {"op": "scan", "table": table, "as": alias}


def gcol(t, c):
    return {"table": t, "column": c}


def print_rows(title, result):
    header = ", ".join(
        (c["source"] + "." if c["source"] else "") + c["name"]
        for c in result["columns"]
    )
    print(f"\n{title}（{len(result['rows'])} 行）")
    print(f"  {header}")
    for row in result["rows"]:
        print(f"  {row}")


def main():
    tables = build_tables()

    # --- 1. 过滤 + 连接之后的两级 grouping sets 汇总
    joined = {
        "op": "inner_join",
        "left": {
            "op": "filter",
            "input": scan("employees", "e"),
            # 过滤掉 NULL 分数（UNKNOWN 不保留），重复行 (1,'A',10) 保留两行
            "expr": cmp(">=", col("e", "score"), lit(0)),
        },
        "right": scan("departments", "d"),
        "on": cmp("=", col("e", "dept"), col("d", "did")),
    }
    plan = {
        "op": "aggregate",
        "input": joined,
        "grouping_sets": [[gcol("d", "dname")], []],
        "aggregates": [
            {"name": "n", "func": "count"},
            {"name": "total", "func": "sum", "expr": col("e", "score")},
        ],
    }
    result = execute(plan, tables)
    print_rows(
        "[两级汇总] filter+join 后 GROUPING SETS ((dname), ())："
        "手算 Alpha=(1,A,10)x2 -> n=2,total=20；Beta -> n=1,total=20；"
        "全表 -> n=3,total=40",
        result,
    )
    print("  位图解读：grouping=1 表示 dname 参与分组；grouping=0 的行为"
          "全表汇总，dname 列的 NULL 是'汇总产生的空列'而非真实 NULL。")

    # --- 2. 空表边界
    empty = {"t": {"columns": ["k", "v"], "rows": []}}
    plan_empty_mixed = {
        "op": "aggregate",
        "input": scan("t", "t"),
        "grouping_sets": [[gcol("t", "k")], []],
        "aggregates": [
            {"name": "n", "func": "count"},
            {"name": "s", "func": "sum", "expr": col("t", "v")},
        ],
    }
    print_rows(
        "[空表] 空输入 + [按 k 分组, 空分组集]：非空集零行，"
        "空分组集仍有一行 count=0、sum=NULL",
        execute(plan_empty_mixed, empty),
    )

    # --- 3. 重复分组集不去重
    plan_dup = {
        "op": "aggregate",
        "input": scan("employees", "e"),
        "grouping_sets": [[gcol("e", "dept")], [gcol("e", "dept")]],
        "aggregates": [{"name": "n", "func": "count"}],
    }
    print_rows(
        "[重复分组集] 同一分组集写两次 => 结果各出一份（袋语义，不去重）；"
        "注意 dept 为 NULL 的组：真实 NULL 且 grouping=1",
        execute(plan_dup, tables),
    )

    # --- 4. 真实触发的拒绝：sum 作用在 bool 列
    bool_tables = {"t": {"columns": ["flag"], "rows": [[True], [False]]}}
    bad_plan = {
        "op": "aggregate",
        "input": scan("t", "t"),
        "grouping_sets": [[]],
        "aggregates": [{"name": "s", "func": "sum", "expr": col("t", "flag")}],
    }
    print("\n[拒绝边界] sum 作用在 bool 列（bool 不视作数值）：")
    try:
        execute(bad_plan, bool_tables)
    except ExpressionError as exc:
        print(f"  按契约拒绝（ExpressionError，是 QueryError 的子类）：\n    {exc}")
    else:  # pragma: no cover
        raise AssertionError("bool 求和必须被拒绝")

    print("\n演示完成：以上结果均为现场计算，无预录、无 sleep。")


if __name__ == "__main__":
    main()

