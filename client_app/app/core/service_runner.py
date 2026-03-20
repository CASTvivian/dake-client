import subprocess
import sys
from typing import Optional


class ServiceProc:
    def __init__(self, name: str):
        self.name = name
        self.proc: Optional[subprocess.Popen] = None

    def start(self, cmd: list[str], env: dict[str, str]) -> None:
        if self.proc and self.proc.poll() is None:
            return
        self.proc = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

    def stop(self) -> None:
        if not self.proc:
            return
        if self.proc.poll() is None:
            self.proc.terminate()
        self.proc = None

    def is_running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None


def python_exe() -> str:
    return sys.executable
