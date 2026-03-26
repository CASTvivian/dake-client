# -*- coding: utf-8 -*-
import os
import sys
from pathlib import Path

# 兼容 `python client_app/app/main_6000.py` 直接启动。
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

def _bootstrap_qt_plugin_path():
    try:
        import PySide6
    except Exception:
        return
    plugin_root = Path(PySide6.__file__).resolve().parent / "Qt" / "plugins"
    platform_root = plugin_root / "platforms"
    if plugin_root.exists():
        os.environ.setdefault("QT_PLUGIN_PATH", str(plugin_root))
    if platform_root.exists():
        os.environ.setdefault("QT_QPA_PLATFORM_PLUGIN_PATH", str(platform_root))

_bootstrap_qt_plugin_path()

from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QApplication, QMainWindow, QLabel, QWidget, QVBoxLayout

APP_NAME = "大可客户端（6000 合并工具）"

def resource_path(rel: str) -> str:
    base = Path(getattr(sys, "_MEIPASS", REPO_ROOT))
    candidates = [
        base / rel,
        base / "client_app" / rel,
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    return str(candidates[0])

class Main6000(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.setWindowIcon(QIcon(resource_path("assets/tray.png")))
        self.resize(900, 600)
        root = QWidget()
        layout = QVBoxLayout(root)
        layout.addWidget(QLabel("这里是 6000 合表工具（独立程序）。\n后续把合表 UI 单独放这里，不和 6002 混用。"))
        self.setCentralWidget(root)

def main():
    app = QApplication(sys.argv)
    w = Main6000()
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
