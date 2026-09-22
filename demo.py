"""固定输入演示：三值逻辑关系查询解释器。

运行: python demo.py   （约 8 秒内完成，纯离线真实计算，无 sleep、无预录结果）

内容:
  1) 两种 filter 的结果差异（dept = 10 与 dept <> 10；NULL 行在两种谓词下
     都因 UNKNOWN 被剔除）；
  2) LEFT JOIN 的 ON 条件 vs 连接后 WHERE 条件差异（同一业务问题两种写法）；
  3) 一个真实触发的异常边界：谓词引用不存在的列 -> SchemaError。
"""

import time

import relational3vl as q


# --- 确定性示例数据（代码内固定，不读取任何外部文件） ------------------------

def build_tables():
    return {
        # 员工：部门 dept 含一个 NULL；左表有一条完全重复的行
        "emp": {
            "columns": [["e", "id"], ["e", "dept"]],
            "rows": [
                [1, 10],
                [2, None],
                [3, 20],
                [1, 10],
                [4, 99],
            ],
        },
        # 部门：id=10 有两条（模拟一对多）
        "dep": {
            "columns": [["d", "id"], ["d", "name"]],
            "rows": [
                [10, "A"],
                [20, "B"],
                [10, "A2"],
            ],
        },
    }


def c(source, name):
    return {"op": "col", "source": source, "name": name}


def k(v):
    return {"op": "lit", "value": v}


def eq(a, b):
    return {"op": "eq", "left": a, "right": b}


def ne(a, b):
    return {"op": "ne", "left": a, "right": b}


def print_table(title, result):
    print(title)
    header = " | ".join(f"{s}.{n}" for s, n in result["columns"])
    print("  " + header)
    print("  " + "-" * max(len(header), 12))
    if not result["rows"]:
        print("  (0 行)")
    for row in result["rows"]:
        print("  " + " | ".join("NULL" if v is None else str(v) for v in row))
    print(f"  -> {len(result['rows'])} 行（带重复行的袋语义）\n")


def main():
    started = time.perf_counter()
    tables = build_tables()
    scan_emp = {"type": "scan", "table": "emp"}

    print("=" * 64)
    print("relational3vl 离线演示（Python 标准库，无 SQL、无数据库）")
    print("=" * 64)

    # --- 过滤结果 1: dept = 10 -----------------------------------------------
    plan_eq10 = {"type": "filter", "input": scan_emp,
                 "expr": eq(c("e", "dept"), k(10))}
    res_eq10 = q.execute(plan_eq10, tables)
    print_table("【过滤一】FILTER  e.dept = 10\n"
                "  只保留 TRUE；dept=20 为 FALSE，dept=NULL 为 UNKNOWN，均剔除",
                res_eq10)
    assert [tuple(r) for r in res_eq10["rows"]] == [(1, 10), (1, 10)]

    # --- 过滤结果 2: dept <> 10 ----------------------------------------------
    # 关键 3VL 语义：NULL <> 10 = UNKNOWN（不是 TRUE），员工 2 仍然不出现。
    plan_ne10 = {"type": "filter", "input": scan_emp,
                 "expr": ne(c("e", "dept"), k(10))}
    res_ne10 = q.execute(plan_ne10, tables)
    print_table("【过滤二】FILTER  e.dept <> 10\n"
                "  NULL <> 10 也是 UNKNOWN 而非 TRUE：NULL 部门员工同样被剔除",
                res_ne10)
    assert [tuple(r) for r in res_ne10["rows"]] == [(3, 20), (4, 99)]

    # --- 附加：LEFT JOIN 的 ON 与连接后 WHERE 差异 ----------------------------
    on_match = eq(c("e", "dept"), c("d", "id"))
    plan_on = {
        "type": "left_join", "left": scan_emp,
        "right": {"type": "scan", "table": "dep"},
        "on": {"op": "and", "left": on_match,
               "right": eq(c("d", "name"), k("A"))},
    }
    print_table("【左连接 A】LEFT JOIN dep ON 部门匹配 AND d.name = 'A'\n"
                "  不满足 ON 的左行保留，右侧补 NULL（员工 2/3/4）",
                q.execute(plan_on, tables))

    plan_where = {
        "type": "filter",
        "input": {
            "type": "left_join", "left": scan_emp,
            "right": {"type": "scan", "table": "dep"}, "on": on_match,
        },
        "expr": eq(c("d", "name"), k("A")),
    }
    print_table("【左连接 B】LEFT JOIN dep ON 部门匹配, WHERE d.name = 'A'\n"
                "  补 NULL 的行在 WHERE 中得到 UNKNOWN，全部被过滤",
                q.execute(plan_where, tables))

    # --- 实际触发的异常边界：非法列 -------------------------------------------
    print("【异常边界】谓词引用不存在的列 e.unknown_col")
    bad_plan = {"type": "filter", "input": scan_emp,
                "expr": eq(c("e", "unknown_col"), k(10))}
    try:
        q.execute(bad_plan, tables)
    except q.SchemaError as exc:
        print(f"  如契约抛出 {type(exc).__name__}: {exc}")
        print("  （预检阶段失败；未修改任何输入对象，也没有部分结果）")
    else:  # pragma: no cover - 防御：未触发即演示失败
        raise AssertionError("非法列应当触发 SchemaError")

    elapsed = time.perf_counter() - started
    print("-" * 64)
    print(f"全部为真实计算，耗时 {elapsed:.3f} 秒（上限约 8 秒）。")
    assert elapsed < 8.0, "演示超时"


if __name__ == "__main__":
    main()
