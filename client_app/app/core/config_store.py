# -*- coding: utf-8 -*-
import json
import sqlite3
from pathlib import Path
from typing import Dict, Any, List

DEFAULT_PRODUCTS = [
    {"code": "SBGZ87", "display_name": "缙云", "enabled": 1},
    {"code": "STR134", "display_name": "新力", "enabled": 1},
    {"code": "SXA927", "display_name": "放10", "enabled": 1},
    {"code": "SXZ218", "display_name": "放8", "enabled": 1},
]

class ConfigStore:
    """
    本地配置/白名单存储（Windows 桌面版）
    - 配置文件：<data_dir>/config/config.json
    - 产品表：  <data_dir>/config/config.db (sqlite)
    """
    def __init__(self, default_data_dir: str, app_id: str):
        self._data_dir = Path(default_data_dir)
        self.app_id = app_id
        self.ensure_runtime_dirs()
        self._init_db()

    @property
    def data_dir(self) -> str:
        return str(self._data_dir)

    def set_data_dir(self, new_dir: str) -> None:
        self._data_dir = Path(new_dir)
        self.ensure_runtime_dirs()
        self._init_db()

    def ensure_runtime_dirs(self) -> None:
        # 6000 / 6002 分开数据目录
        (self._data_dir / "config").mkdir(parents=True, exist_ok=True)
        (self._data_dir / "logs").mkdir(parents=True, exist_ok=True)
        (self._data_dir / "services" / "nav_report").mkdir(parents=True, exist_ok=True)
        (self._data_dir / "services" / "xlsx_merge").mkdir(parents=True, exist_ok=True)

    # ---------------- config.json ----------------
    def _config_path(self) -> Path:
        return self._data_dir / "config" / "config.json"

    def load(self) -> Dict[str, Any]:
        p = self._config_path()
        if not p.exists():
            cfg = {
                "imap_host": "imap.qq.com",
                "imap_user": "",
                "imap_pass": "",
                "lookback_days": 3,
                "folder_mode": "all",   # all / list
                "folders": ["INBOX"],
                "folders_blacklist": "Deleted,Trash,Junk,Spam,垃圾,已删除,Drafts,草稿,Sent,已发送",
                "push_enabled": True,
                "wecom_webhook_url": "",
                # 本地服务端口（避免与客户电脑其它服务冲突）
                "nav_port": 16002,
                "xlsx_port": 16000,
            }
            self.save(cfg)
            return cfg
        return json.loads(p.read_text(encoding="utf-8"))

    def save(self, cfg: Dict[str, Any]) -> None:
        p = self._config_path()
        p.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")

    # ---------------- products sqlite ----------------
    def _db_path(self) -> Path:
        return self._data_dir / "config" / "config.db"

    def _init_db(self) -> None:
        with sqlite3.connect(self._db_path()) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS product_allowlist(
                  code TEXT PRIMARY KEY,
                  display_name TEXT NOT NULL,
                  enabled INTEGER NOT NULL DEFAULT 1
                )
            """)
            # 初始化：若空表则写默认四个
            cur = conn.execute("SELECT COUNT(*) FROM product_allowlist")
            n = cur.fetchone()[0]
            if n == 0:
                for it in DEFAULT_PRODUCTS:
                    conn.execute(
                        "INSERT INTO product_allowlist(code, display_name, enabled) VALUES (?,?,?)",
                        (it["code"], it["display_name"], it["enabled"])
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
            conn.execute("""
                INSERT INTO product_allowlist(code, display_name, enabled)
                VALUES (?,?,?)
                ON CONFLICT(code) DO UPDATE SET
                  display_name=excluded.display_name,
                  enabled=excluded.enabled
            """, (code, display_name, int(enabled)))
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
            newv = 0 if int(row[0]) == 1 else 1
            conn.execute("UPDATE product_allowlist SET enabled=? WHERE code=?", (newv, code))
            conn.commit()

    def products_delete(self, code: str) -> None:
        code = (code or "").strip().upper()
        if not code:
            return
        with sqlite3.connect(self._db_path()) as conn:
            conn.execute("DELETE FROM product_allowlist WHERE code=?", (code,))
            conn.commit()
