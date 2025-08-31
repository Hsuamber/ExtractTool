
from __future__ import annotations
import importlib
import inspect
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, List, Optional

from .utils import ensure_dir, Logger

# ---- Types ----

@dataclass
class Output:
    message: str = ""
    artifacts: List[str] = None  # 例如輸出的檔案路徑
    def __post_init__(self):
        if self.artifacts is None:
            self.artifacts = []

@dataclass
class Task:
    id: str
    name: str
    description: str
    handler: Callable[["TaskContext"], Output]

@dataclass
class TaskContext:
    game: str
    out_root: str
    out_dir: str
    now: datetime
    logger: Logger

# ---- Global Registry ----

_REGISTRY: Dict[str, Dict[str, Task]] = {
    # "gsi": { "fetch_characters": Task(...), ... }
}

def task(game: str, id: str, name: str, desc: str):
    """裝飾器：註冊任務到全域 Registry。"""
    def decorator(fn: Callable[[TaskContext], Output]):
        _REGISTRY.setdefault(game, {})
        if id in _REGISTRY[game]:
            raise ValueError(f"Duplicate task id for game '{game}': {id}")
        _REGISTRY[game][id] = Task(id=id, name=name, description=desc, handler=fn)
        return fn
    return decorator

def get_games() -> List[str]:
    return sorted(_REGISTRY.keys())

def get_tasks(game: str) -> Dict[str, Task]:
    return _REGISTRY.get(game, {})

def load_game_module(game: str):
    """延遲載入 games.{game}，以觸發 @task 註冊。"""
    mod_name = f"crawler_skeleton.games.{game}"
    importlib.import_module(mod_name)

def ensure_output_dirs(game: str, out_root: str) -> str:
    out_dir = os.path.join(out_root, game, datetime.now().strftime("%Y%m%d_%H%M%S"))
    ensure_dir(out_dir)
    return out_dir

def run_task(game: str, task_id: str, out_root: str = "outputs") -> Output:
    if game not in _REGISTRY:
        raise ValueError(f"Game '{game}' not loaded or has no tasks.")
    task = _REGISTRY[game].get(task_id)
    if not task:
        raise ValueError(f"Task '{task_id}' not found for game '{game}'.")
    out_dir = ensure_output_dirs(game, out_root)
    ctx = TaskContext(game=game, out_root=out_root, out_dir=out_dir, now=datetime.now(), logger=Logger(name=f"{game}:{task_id}"))
    ctx.logger.info(f"Start task: {task.name} ({task.id}) -> out={out_dir}")
    result = task.handler(ctx)
    ctx.logger.info(f"Done: {result.message}")
    return result
