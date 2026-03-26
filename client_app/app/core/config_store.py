# -*- coding: utf-8 -*-
import json
import sqlite3
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Any, List

DEFAULT_PRODUCTS = [
    {"code": "SBGZ87", "display_name": "缙云", "enabled": 1},
    {"code": "STR134", "display_name": "新力", "enabled": 1},
    {"code": "SXA927", "display_name": "放10", "enabled": 1},
    {"code": "SXZ218", "display_name": "放8", "enabled": 1},
]

DEFAULT_DATA_ROOT_WIN = r"C:\DakeClient\data"
DEFAULT_PUSH_SLOTS = "14:00,16:00,16:46"


@dataclass
class AppPaths:
    data_root: str
    data_6002: str
    data_6000: str
    nav_data: str
    merge_data: str
    log_dir: str
    config_dir: str
    nav_out: str
    merge_out: str


class ConfigStore:
    """
    本地配置/白名单存储（Windows 桌面版）
    - 配置文件：<data_root>/config/config.json
    - 产品表：  <data_root>/config/config.db (sqlite)
    - 数据目录：
        <data_root>/6002/nav_report
        <data_root>/6000/xlsx_merge
    """

    def __init__(self, default_data_dir: str, app_id: str):
        self.app_id = app_id
        self._data_root = Path(default_data_dir)
        self.ensure_split_dirs()
        self._init_db()

    @property
    def data_dir(self) -> str:
        return str(self._data_root)

    def get_data_root(self) -> str:
        return str(self._data_root)

    def set_data_root(self, new_dir: str) -> None:
        p = Path((new_dir or "").strip() or DEFAULT_DATA_ROOT_WIN)
        self._data_root = p
        self.ensure_split_dirs()
        self._init_db()

    def set_data_dir(self, new_dir: str) -> None:
        self.set_data_root(new_dir)

    def get_paths(self) -> AppPaths:
        data_root = self._data_root
        data_6002 = data_root / "6002"
        data_6000 = data_root / "6000"
        nav_data = data_6002 / "nav_report"
        merge_data = data_6000 / "xlsx_merge"
        log_dir = data_root / "logs"
        config_dir = data_root / "config"
        nav_out = nav_data / "out"
        merge_out = merge_data / "out"
        return AppPaths(
            data_root=str(data_root),
            data_6002=str(data_6002),
            data_6000=str(data_6000),
            nav_data=str(nav_data),
            merge_data=str(merge_data),
            log_dir=str(log_dir),
            config_dir=str(config_dir),
            nav_out=str(nav_out),
            merge_out=str(merge_out),
        )

    def ensure_split_dirs(self) -> None:
        paths = self.get_paths()
        for p in [
            paths.data_root,
            paths.data_6002,
            paths.data_6000,
            paths.nav_data,
            paths.merge_data,
            paths.log_dir,
            paths.config_dir,
            paths.nav_out,
            paths.merge_out,
            str(Path(paths.nav_data) / "raw"),
        ]:
            Path(p).mkdir(parents=True, exist_ok=True)

    def ensure_runtime_dirs(self) -> None:
        self.ensure_split_dirs()

    def _default_config(self) -> Dict[str, Any]:
        return {
            "data_root": self.get_data_root(),
            "imap_host": "imap.qq.com",
            "imap_user": "",
            "imap_pass": "",
            "mail_lookback_days": 3,
            "imap_folders_mode": "all",
            "imap_folders": "INBOX",
            "imap_folders_blacklist": "Deleted,Trash,Junk,Spam,垃圾,已删除,Drafts,草稿,Sent,已发送",
            "wecom_webhook_url": "",
            "push_enabled": True,
            "push_slots": DEFAULT_PUSH_SLOTS,
            "nav_port": 16002,
            "xlsx_port": 16000,
            "merge_target_amount": 5000000,
        }

    def _config_path(self) -> Path:
        return self._data_root / "config" / "config.json"

    def load(self) -> Dict[str, Any]:
        self.ensure_split_dirs()
        p = self._config_path()
        base = self._default_config()
        if not p.exists():
            self.save(base)
            return base
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        cfg = dict(base)
        cfg.update(data or {})
        cfg["data_root"] = cfg.get("data_root") or self.get_data_root()
        return cfg

    def save(self, cfg: Dict[str, Any]) -> None:
        merged = self._default_config()
        merged.update(cfg or {})
        data_root = (merged.get("data_root") or self.get_data_root()).strip()
        self._data_root = Path(data_root)
        self.ensure_split_dirs()
        p = self._config_path()
        p.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")

    def export_paths(self) -> Dict[str, Any]:
        return asdict(self.get_paths())

    def _db_path(self) -> Path:
        return self._data_root / "config" / "config.db"

    def _init_db(self) -> None:
        with sqlite3.connect(self._db_path()) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS product_allowlist(
                  code TEXT PRIMARY KEY,
                  display_name TEXT NOT NULL,
                  enabled INTEGER NOT NULL DEFAULT 1
                )
                """
            )
            cur = conn.execute("SELECT COUNT(*) FROM product_allowlist")
            n = cur.fetchone()[0]
            if n == 0:
                for it in DEFAULT_PRODUCTS:
                    conn.execute(
                        "INSERT INTO product_allowlist(code, display_name, enabled) VALUES (?,?,?)",
                        (it["code"], it["display_name"], it["enabled"]),
                    )
            conn.commit()

    def products_list(self) -> List[Dict[str, Any]]:
        with sqlite3.connect(self._db_path()) as conn:
            cur = conn.execute("SELECT code, display_name, enabled FROM product_allowlist ORDER BY code")
            return [{"code": r[0], "display_name": r[1], "enabled": int(r[2])} for r in cur.fetchall()]

    def products_enabled_codes(self) -> List[str]:
        with sqlite3.connect(self._db_path()) as conn:
            cur = conn.execute("SELECT code FROM product_allowlist WHERE enabled=1 ORDER BY code")
            return [r[0] for r in cur.fetchall()]

    def products_upsert(self, code: str, display_name: str, enabled: int = 1) -> None:
        code = (code or "").strip().upper()
        if not code:
            return
        display_name = (display_name or code).strip()
        with sqlite3.connect(self._db_path()) as conn:
            conn.execute(
                """
                INSERT INTO product_allowlist(code, display_name, enabled)
                VALUES (?,?,?)
                ON CONFLICT(code) DO UPDATE SET
                  display_name=excluded.display_name,
                  enabled=excluded.enabled
                """,
                (code, display_name, int(enabled)),
            )
            conn.commit()

    def products_toggle(self, code: str) -> None:
        code = (code or "").strip().upper()
        if not code:
            return
        with sqlite3.connect(self._db_path()) as conn:
            cur = conn.execute("SELECT enabled FROM product_allowlist WHERE code=?", (code,))
            row = cur.fetchone()
            if not row:
                return
            conn.execute("UPDATE product_allowlist SET enabled=? WHERE code=?", (0 if int(row[0]) == 1 else 1, code))
            conn.commit()

    def products_delete(self, code: str) -> None:
        code = (code or "").strip().upper()
        if not code:
            return
        with sqlite3.connect(self._db_path()) as conn:
            conn.execute("DELETE FROM product_allowlist WHERE code=?", (code,))
            conn.commit()
