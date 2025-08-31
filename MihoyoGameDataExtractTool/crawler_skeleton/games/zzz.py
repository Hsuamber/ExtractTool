
"""絕區零（zzz）示範任務。"""
from __future__ import annotations
import os, json
from ..core import task, TaskContext, Output

@task(game="zzz", id="fetch_agents", name="代理人清單", desc="下載 ZZZ 代理人清單（示範）")
def fetch_agents(ctx: TaskContext) -> Output:
    data = {"agents": ["Anby", "Nicole", "Billy", "Ellen" ]}
    out_file = os.path.join(ctx.out_dir, "zzz_agents_demo.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return Output(message=f"寫出 {out_file}", artifacts=[out_file])

@task(game="zzz", id="fetch_merch", name="周邊商品（示範）", desc="下載 ZZZ 商品清單（示範）")
def fetch_merch(ctx: TaskContext) -> Output:
    out_dir = ctx.out_dir
    path = os.path.join(out_dir, "zzz_merch_demo.tsv")
    with open(path, "w", encoding="utf-8") as f:
        f.write("sku\tname\tprice\n")
        f.write("A001\tKeychain\t199\n")
        f.write("A002\tAcrylic Stand\t349\n")
    return Output(message=f"寫出 {path}", artifacts=[path])
