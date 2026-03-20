import json
from pathlib import Path
import tkinter as tk
from tkinter import messagebox

BASE_DIR = Path(__file__).resolve().parent.parent
RUNTIME_DIR = Path.cwd()
CONFIG_PATH = RUNTIME_DIR / "config" / "config.json"


def load_config():
    if not CONFIG_PATH.exists():
        return {}
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def main():
    cfg = load_config()
    root = tk.Tk()
    root.title("Dake Client")

    tk.Label(root, text="Dake Client (Windows)", font=("Arial", 14)).pack(pady=10)
    tk.Label(root, text=f"Config: {CONFIG_PATH}").pack(pady=5)

    def show_cfg():
        messagebox.showinfo("Config", json.dumps(cfg, ensure_ascii=False, indent=2))

    tk.Button(root, text="查看当前配置", command=show_cfg, width=20).pack(pady=10)
    tk.Button(root, text="退出", command=root.destroy, width=20).pack(pady=10)

    root.geometry("460x220")
    root.mainloop()


if __name__ == "__main__":
    main()
