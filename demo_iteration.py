"""grouping_sets 增量演示（离线、确定性、纯标准库）。

运行：``python demo_iteration.py``（实测远低于 90 秒预算，现场计算、无 sleep、无预录）。

演示内容：
1. 两级小汇总（城市级 + 区域级 + 总计）手算可核对的结果；
2. grouping 位图区分"汇总空列"与输入真实 NULL；
3. 空输入：空分组集仍有一行（count=0、sum=NULL），非空分组集零行；
4. 重复分组集不去重，各自产出；
5. 一个真实触发的拒绝：sum 作用在字符串列上。
"""

from __future__ import annotations

from rel3vl import QueryError, execute
from rel3vl.errors import ExpressionError


def build_tables():
    # 销售袋：含完全重复行、NULL 金额、同值 NULL 区域（确定性数据，无随机源）
    return {
        "sales": {
            "columns": ["region", "city", "amount"],
            "rows": [
                ["E", "BJ", 10],
                ["E", "BJ", 10],   # 重复行：袋语义下参与两次
                ["E", "TJ", None], # 金额未知
                ["W", "CD", 7],
                [None, None, 3],   # 区域/城市均未知（真实 NULL）
                [None, None, 5],   # 同值 NULL：与上一行同组
            ],
        }
    }


def col(t, c):
    return {"kind": "col", "table": t, "column": c}


def grouping_plan(sets, aggs=None):
    return {
        "op": "grouping_sets",
        "input": {"op": "scan", "table": "sales", "as": "s"},
        "group_by": [{"table": "s", "column": "region"}, {"table": "s", "column": "city"}],
        "sets": sets,
        "aggregates": aggs or [
            {"name": "n", "func": "count"},
            {"name": "amount_n", "func": "count", "expr": col("s", "amount")},
            {"name": "total", "func": "sum", "expr": col("s", "amount")},
        ],
    }


def print_rows(title, result):
    header = ", ".join(
        (c["source"] + "." if c["source"] else "") + c["name"] for c in result["columns"]
    )
    print(f"\n{title}（{len(result['rows'])} 行）")
    print(f"  {header}")
    for row in result["rows"]:
        print(f"  {row}")


def main():
    tables = build_tables()

    # --- 两级小汇总：城市级(11) + 区域级(10) + 总计(00)
    print_rows(
        "[两级汇总] sets=[[region,city],[region],[]]：位图标出哪一级",
        execute(grouping_plan([["region", "city"], ["region"], []]), tables),
    )
    print("  读法：grouping=11 城市级；10 区域级（city 为汇总空列）；00 总计。")
    print("  注意 (NULL,NULL,'11',...) 是真实 NULL 组，与 10/00 行的空列由位图区分。")

    # --- 重复分组集不去重
    print_rows(
        "[重复分组集] sets=[[region],[region]]：两份结果都保留",
        execute(grouping_plan([["region"], ["region"]]), tables),
    )

    # --- 空输入：空分组集一行，非空分组集零行
    empty = {"sales": {"columns": ["region", "city", "amount"], "rows": []}}
    print_rows(
        "[空输入] sets=[[]]：仍有一行 count=0、sum=NULL",
        execute(grouping_plan([[]]), empty),
    )
    print_rows(
        "[空输入] sets=[[region]]：没有组，返回零行",
        execute(grouping_plan([["region"]]), empty),
    )

    # --- 真实触发的拒绝：sum 作用在字符串列
    print("\n[拒绝边界] sum(city) 作用在字符串列：")
    bad = grouping_plan([[]], aggs=[{"name": "x", "func": "sum", "expr": col("s", "city")}])
    try:
        execute(bad, tables)
    except ExpressionError as exc:
        print(f"  按契约拒绝（ExpressionError，是 QueryError 的子类）：\n    {exc}")
    else:  # pragma: no cover
        raise AssertionError("sum(字符串) 必须被拒绝")

    print("\n演示完成：以上结果均为现场计算，无预录、无 sleep。")


if __name__ == "__main__":
    main()
