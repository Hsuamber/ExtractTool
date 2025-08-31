
"""崩壞：星穹鐵道（hsr）示範任務。"""
from __future__ import annotations
import csv, os
from ..core import task, TaskContext, Output

@task(game="hsr", id="fetch_trailblazers", name="角色清單", desc="下載 HSR 角色清單（示範）")
def fetch_trailblazers(ctx: TaskContext) -> Output:
    rows = [
        ["Name", "Path"],
        ["Trailblazer", "Destruction"],
        ["Welt", "Nihility"],
        ["Kafka", "Nihility"],
        ["Jingliu", "Destruction"],
    ]
    out_file = os.path.join(ctx.out_dir, "hsr_characters_demo.csv")
    with open(out_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    return Output(message=f"寫出 {out_file}", artifacts=[out_file])

@task(game="hsr", id="fetch_banners", name="UP 歷史", desc="下載 HSR 卡池（示範）")
def fetch_banners(ctx: TaskContext) -> Output:
    out_txt = os.path.join(ctx.out_dir, "hsr_banners_demo.txt")
    with open(out_txt, "w", encoding="utf-8") as f:
        f.write("2023-04 Kafka, 2023-10 Jingliu, 2024-03 Aventurine\n")
    return Output(message=f"寫出 {out_txt}", artifacts=[out_txt])
