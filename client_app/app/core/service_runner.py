# -*- coding: utf-8 -*-
import os
import sys
import time
import json
import threading
import importlib.util
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

import requests
import uvicorn

def _load_fastapi_app_from_file(py_file: Path, attr: str = "app"):
    spec = importlib.util.spec_from_file_location(py_file.stem, str(py_file))
    if not spec or not spec.loader:
        raise RuntimeError(f"无法加载模块: {py_file}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    app = getattr(mod, attr, None)
    if app is None:
        raise RuntimeError(f"模块缺少 {attr}: {py_file}")
    return app

class _UvicornThread:
    def __init__(self, app, host: str, port: int, log_level: str = "info"):
        self.app = app
        self.host = host
        self.port = port
        self.log_level = log_level
        self.server: Optional[uvicorn.Server] = None
        self.thread: Optional[threading.Thread] = None

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        cfg = uvicorn.Config(self.app, host=self.host, port=self.port, log_level=self.log_level)
        self.server = uvicorn.Server(cfg)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()

    def stop(self):
        if self.server:
            self.server.should_exit = True

class LocalServiceRunner:
    """
    桌面端：本地起 2 个 FastAPI 服务（nav_report/xlsx_merge）
    并提供：
    - apply_config: 写入本地 nav_report 的 .env（供服务读取）
    - call_nav_fetch_probe: 启动自检试抓（不推送）
    - call_nav_process_slot: 触发 14:00/16:00/16:46
    """
    def __init__(self, data_dir: str, app_id: str = "6002"):
        self.data_dir = Path(data_dir)
        self.app_id = app_id

        self.nav_port = 16002
        self.xlsx_port = 16000

        self._nav = None
        self._xlsx = None

        self._nav_url = None
        self._xlsx_url = None

        self._svc_root = Path(__file__).resolve().parents[1] / "services"
        self._nav_root = self._svc_root / "nav_report"
        self._xlsx_root = self._svc_root / "xlsx_merge"

    def _nav_main_file(self) -> Path:
        # 兼容两种结构：
        # services/nav_report/app/main.py 或 services/nav_report/main.py
        p1 = self._nav_root / "app" / "main.py"
        p2 = self._nav_root / "main.py"
        return p1 if p1.exists() else p2

    def _xlsx_main_file(self) -> Path:
        p1 = self._xlsx_root / "app" / "main.py"
        p2 = self._xlsx_root / "main.py"
        return p1 if p1.exists() else p2

    def start_all(self):
        # 尝试从 config.json 覆盖端口
        cfgp = self.data_dir / "config" / "config.json"
        cfg = {}
        if cfgp.exists():
            try:
                cfg = json.loads(cfgp.read_text(encoding="utf-8"))
                self.nav_port = int(cfg.get("nav_port", self.nav_port))
                self.xlsx_port = int(cfg.get("xlsx_port", self.xlsx_port))
            except Exception:
                cfg = {}

        self._nav_url = f"http://127.0.0.1:{self.nav_port}"
        self._xlsx_url = f"http://127.0.0.1:{self.xlsx_port}"

        # 服务模块在 import 阶段就会读取环境变量，必须先应用本地配置。
        self.apply_config(cfg)

        # nav_report
        if self._nav is None:
            nav_app = _load_fastapi_app_from_file(self._nav_main_file(), "app")
            self._nav = _UvicornThread(nav_app, "127.0.0.1", self.nav_port, log_level="info")
            self._nav.start()

        # xlsx_merge（暂时先起服务，后续再接 UI）
        if self._xlsx is None and self._xlsx_main_file().exists():
            try:
                xlsx_app = _load_fastapi_app_from_file(self._xlsx_main_file(), "app")
                self._xlsx = _UvicornThread(xlsx_app, "127.0.0.1", self.xlsx_port, log_level="info")
                self._xlsx.start()
            except Exception:
                # 合表服务失败也不阻塞 6002 主链路
                self._xlsx = None

        # 等待 nav health
        self._wait_health(self._nav_url + "/health", timeout=25)

    def stop_all(self):
        if self._nav:
            self._nav.stop()
        if self._xlsx:
            self._xlsx.stop()

    def status(self) -> Dict[str, str]:
        st = {"nav_report": "DOWN", "xlsx_merge": "DOWN"}
        if self._nav_url and self._is_up(self._nav_url + "/health"):
            st["nav_report"] = "UP"
        if self._xlsx_url and self._is_up(self._xlsx_url + "/health"):
            st["xlsx_merge"] = "UP"
        return st

    def apply_config(self, cfg: Dict[str, Any]):
        """
        把桌面端配置写入 nav_report 的运行目录（本地保存全部文件）
        """
        # 本地 nav_report 数据目录
        nav_data = self.data_dir / "services" / "nav_report"
        nav_data.mkdir(parents=True, exist_ok=True)

        # 生成 .env（服务会用 python-dotenv 读取）
        # 产品白名单：禁用后报表完全不出现
        allowlist_codes = cfg.get("allowlist_codes", None)
        if not allowlist_codes:
            # 从本地 config.db 取 enabled=1 的 code
            try:
                import sqlite3
                dbp = self.data_dir / "config" / "config.db"
                with sqlite3.connect(dbp) as conn:
                    cur = conn.execute("SELECT code FROM product_allowlist WHERE enabled=1 ORDER BY code")
                    allowlist_codes = ",".join([r[0] for r in cur.fetchall()])
            except Exception:
                allowlist_codes = ""

        env_lines = [
            f"DATA_DIR={nav_data.as_posix()}",
            f"IMAP_HOST={cfg.get('imap_host','imap.qq.com')}",
            f"IMAP_USER={cfg.get('imap_user','')}",
            f"IMAP_PASS={cfg.get('imap_pass','')}",
            f"IMAP_FOLDER=INBOX",
            f"MAIL_LOOKBACK_DAYS={int(cfg.get('lookback_days',3))}",
            f"IMAP_FOLDERS_MODE={cfg.get('folder_mode','all')}",
            f"IMAP_FOLDERS_BLACKLIST={cfg.get('folders_blacklist','')}",
            f"WECOM_PUSH_ENABLED={'1' if cfg.get('push_enabled',True) else '0'}",
            f"WECOM_WEBHOOK_URL={cfg.get('wecom_webhook_url','')}",
            f"PRODUCT_CODE_ALLOWLIST={allowlist_codes or ''}",
            # slot
            "PUSH_SLOTS=14:00,16:00,16:46",
        ]
        (nav_data / ".env").write_text("\n".join(env_lines) + "\n", encoding="utf-8")

        # 当前进程也同步设置，保证通过 importlib 启动服务时能读到正确路径。
        env_map = {
            "DATA_DIR": nav_data.as_posix(),
            "IMAP_HOST": str(cfg.get("imap_host", "imap.qq.com")),
            "IMAP_USER": str(cfg.get("imap_user", "")),
            "IMAP_PASS": str(cfg.get("imap_pass", "")),
            "IMAP_FOLDER": "INBOX",
            "MAIL_LOOKBACK_DAYS": str(int(cfg.get("lookback_days", 3))),
            "IMAP_FOLDERS_MODE": str(cfg.get("folder_mode", "all")),
            "IMAP_FOLDERS_BLACKLIST": str(cfg.get("folders_blacklist", "")),
            "WECOM_PUSH_ENABLED": "1" if cfg.get("push_enabled", True) else "0",
            "WECOM_WEBHOOK_URL": str(cfg.get("wecom_webhook_url", "")),
            "PRODUCT_CODE_ALLOWLIST": allowlist_codes or "",
            "PUSH_SLOTS": "14:00,16:00,16:46",
        }
        os.environ.update(env_map)

    def test_webhook(self, url: str) -> Tuple[bool, str]:
        try:
            r = requests.post(url, json={"msgtype":"markdown","markdown":{"content":"【大可客户端】Webhook 测试 ✅"}}, timeout=10)
            if r.status_code != 200:
                return False, f"HTTP {r.status_code}"
            j = r.json()
            if int(j.get("errcode", -1)) == 0:
                return True, "ok"
            return False, str(j)
        except Exception as e:
            return False, str(e)

    def call_nav_fetch_probe(self, push: bool = False) -> Dict[str, Any]:
        """
        启动自检试抓：不推送，允许窗口外抓（force）
        """
        self.start_all()
        payload = {
            "force": True,
            "strict": False,
            "push": False if not push else True,
            "lookback_days": 3,
            "target_val_date_only": False,
        }
        # 优先用 fetch_backfill（不会被窗口限制）
        url = self._nav_url + "/api/fetch_backfill"
        r = requests.post(url, json=payload, timeout=60)
        try:
            return r.json()
        except Exception:
            return {"ok": False, "status_code": r.status_code, "body": r.text}

    def call_nav_process_slot(self, slot: str, push: bool, force: bool = True) -> Dict[str, Any]:
        self.start_all()
        url = self._nav_url + "/api/process_slot"
        payload = {"slot": slot, "force": bool(force), "push": bool(push)}
        r = requests.post(url, json=payload, timeout=180)
        try:
            return r.json()
        except Exception:
            return {"ok": False, "status_code": r.status_code, "body": r.text}

    # ------------- helpers -------------
    def _is_up(self, url: str) -> bool:
        try:
            r = requests.get(url, timeout=3)
            return r.status_code == 200
        except Exception:
            return False

    def _wait_health(self, url: str, timeout: int = 20):
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self._is_up(url):
                return
            time.sleep(0.4)
        # 不中断，由上层自检提示
