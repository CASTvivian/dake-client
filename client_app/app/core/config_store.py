import json
import os
from typing import Any, Dict

DEFAULT_DATA_ROOT = r"C:\DakeClient\data"


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def default_config() -> Dict[str, Any]:
    return {
        "data_root": DEFAULT_DATA_ROOT,
        "services": {"nav_report": {"port": 6002}, "xlsx_merge": {"port": 6000}},
        "imap": {
            "host": "imap.qq.com",
            "user": "",
            "pass": "",
            "folder_mode": "all",
            "folders": ["INBOX"],
            "blacklist_keywords": ["Deleted", "Trash", "Junk", "Spam", "垃圾", "已删除", "Drafts", "草稿", "Sent", "已发送"],
            "lookback_days": 3,
        },
        "wecom": {"push_enabled": True, "webhook_url": ""},
        "push_slots": ["14:00", "16:00", "16:46"],
        "products": [
            {"code": "SBGZ87", "display_name": "缙云", "enabled": 1},
            {"code": "STR134", "display_name": "新力", "enabled": 1},
            {"code": "SXA927", "display_name": "放10", "enabled": 1},
            {"code": "SXZ218", "display_name": "放8", "enabled": 1},
        ],
    }


def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        cfg = default_config()
        _ensure_dir(os.path.dirname(path))
        save_config(path, cfg)
        return cfg
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_config(path: str, cfg: Dict[str, Any]) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def resolve_runtime_paths(data_root: str) -> Dict[str, str]:
    return {
        "data_root": data_root,
        "nav_report_root": os.path.join(data_root, "nav_report"),
        "xlsx_merge_root": os.path.join(data_root, "xlsx_merge"),
        "calendar_root": os.path.join(data_root, "calendar"),
        "logs_root": os.path.join(data_root, "logs"),
    }


def ensure_runtime_dirs(data_root: str) -> Dict[str, str]:
    paths = resolve_runtime_paths(data_root)
    for key, path in paths.items():
        if key.endswith('_root') or key == 'data_root':
            _ensure_dir(path)
    _ensure_dir(os.path.join(paths['nav_report_root'], 'raw'))
    _ensure_dir(os.path.join(paths['nav_report_root'], 'out'))
    _ensure_dir(os.path.join(paths['xlsx_merge_root'], 'out'))
    return paths
