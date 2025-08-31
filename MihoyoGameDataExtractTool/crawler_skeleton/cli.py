
from __future__ import annotations
import sys
from typing import List

from .core import get_games, get_tasks, load_game_module, run_task

WELCOME = """
== 爬蟲骨幹 Skeleton ==
選擇遊戲（輸入代碼）：
  1) gsi  - 原神 Genshin Impact
  2) hsr  - 崩壞：星穹鐵道 Honkai: Star Rail
  3) zzz  - 絕區零 Zenless Zone Zero
q) 離開
"""

def _prompt(text: str) -> str:
    try:
        return input(text).strip()
    except EOFError:
        return ""

def _pick_game() -> str:
    while True:
        print(WELCOME)
        choice = _prompt("> ").lower()
        mapping = {"1": "gsi", "2": "hsr", "3": "zzz", "gsi": "gsi", "hsr": "hsr", "zzz": "zzz"}
        if choice in ("q", "quit", "exit"): sys.exit(0)
        if choice in mapping:
            return mapping[choice]
        print("無效選擇，請重新輸入。\n")

def _pick_tasks(game: str) -> List[str]:
    # 延遲載入遊戲模組，觸發 @task 註冊
    load_game_module(game)
    tasks = get_tasks(game)
    if not tasks:
        print(f"遊戲 {game} 目前無任務可執行。\n")
        return []
    print(f"\n== {game} 任務清單 ==")
    ordered = sorted(tasks.values(), key=lambda t: t.id)
    for idx, t in enumerate(ordered, 1):
        print(f"  {idx}) {t.id:<22} {t.name} — {t.description}")
    print("  a) 全部任務\n")
    raw = _prompt("輸入欲執行的編號或 task_id（可逗號分隔），或按 Enter 跳過： ")
    if not raw:
        return []
    raw = raw.strip().lower()
    if raw in ("a", "all"):
        return [t.id for t in ordered]
    # 支援混合：數字或 task_id
    picked = []
    for token in (x.strip() for x in raw.split(",") if x.strip()):
        if token.isdigit():
            i = int(token) - 1
            if 0 <= i < len(ordered):
                picked.append(ordered[i].id)
        elif token in tasks:
            picked.append(token)
        else:
            print(f"忽略未知選項：{token}")
    # 去重保序
    seen = set(); uniq = []
    for tid in picked:
        if tid not in seen:
            seen.add(tid); uniq.append(tid)
    return uniq

def run_cli():
    game = _pick_game()
    tids = _pick_tasks(game)
    if not tids:
        print("未選擇任務。結束。\n")
        return
    print(f"\n將執行 {game} 的任務：{', '.join(tids)}\n")
    for tid in tids:
        try:
            run_task(game, tid, out_root="outputs")
        except Exception as e:
            print(f"[ERROR] 任務 {tid} 失敗：{e}")
    print("\n所有選定任務已嘗試執行。\n")
