
"""原神（gsi）示範任務。這裡僅用示範資料與假流程。"""
from __future__ import annotations
import json, os
from datetime import datetime
from ..core import task, TaskContext, Output

@task(game="gsi", id="fetch_characters", name="角色清單", desc="下載原神角色清單（示範）")
def fetch_characters(ctx: TaskContext) -> Output:
    data = {
        "game": ctx.game,
        "generated_at": ctx.now.isoformat(),
        "characters": ["Aether/Lumine", "Amber", "Venti", "Zhongli", "Raiden Shogun"]
    }
    out_file = os.path.join(ctx.out_dir, "gsi_characters_demo.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return Output(message=f"寫出 {out_file}", artifacts=[out_file])

@task(game="gsi", id="fetch_banners", name="UP 歷史", desc="下載卡池 UP 歷史（示範）")
def fetch_banners(ctx: TaskContext) -> Output:
    demo = {
        "banners": [
            {"phase": "1.0", "chars": ["Venti"]},
            {"phase": "1.1", "chars": ["Klee"]},
            {"phase": "1.2", "chars": ["Kazuha"]},
        ]
    }
    out_file = os.path.join(ctx.out_dir, "gsi_banners_demo.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(demo, f, ensure_ascii=False, indent=2)
    return Output(message=f"寫出 {out_file}", artifacts=[out_file])
