
import os
import sys
from datetime import datetime

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)
    return path

class Logger:
    def __init__(self, name: str = "crawler", stream=None):
        self.name = name
        self.stream = stream or sys.stdout

    def _log(self, level: str, msg: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.stream.write(f"[{ts}] {self.name:<10} {level:<5} | {msg}\n")
        self.stream.flush()

    def info(self, msg: str): self._log("INFO", msg)
    def warn(self, msg: str): self._log("WARN", msg)
    def error(self, msg: str): self._log("ERROR", msg)
