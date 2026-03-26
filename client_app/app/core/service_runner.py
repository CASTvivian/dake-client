# CODEX_DISABLE_UVICORN_DEFAULT_LOGGING
# -*- coding: utf-8 -*-
import datetime
import importlib.util
import json
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import requests
import uvicorn


def resource_path(*parts: str) -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        base = Path(sys._MEIPASS)
    else:
        base = Path(__file__).resolve().parents[3]
    return base.joinpath(*parts)


def _load_fastapi_app_from_file(py_file: Path, attr: str = "app", module_name: str | None = None):
    name = module_name or f"{py_file.stem}_{int(time.time() * 1000)}"
    spec = importlib.util.spec_from_file_location(name, str(py_file))
    if not spec or not spec.loader:
        raise RuntimeError(f"无法加载模块: {py_file}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    app = getattr(mod, attr, None)
    if app is None:
        raise RuntimeError(f"模块缺少 {attr}: {py_file}")
    return app


class _UvicornThread:
    def __init__(self, app, host: str, port: int, log_level: str = "warning"):
        self.app = app
        self.host = host
        self.port = port
        self.log_level = log_level
        self.server: Optional[uvicorn.Server] = None
        self.thread: Optional[threading.Thread] = None

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        cfg = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            log_level=self.log_level,
            log_config=None,
            access_log=False,
        )
        self.server = uvicorn.Server(cfg)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()

    def stop(self):
        if self.server:
            self.server.should_exit = True
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)


class LocalServiceRunner:
    def __init__(self, data_dir: str, app_id: str = "6002"):
        self.data_root = Path(data_dir)
        self.app_id = app_id
        self.nav_port = 16002
        self.xlsx_port = 16000
        self._nav: Optional[_UvicornThread] = None
        self._xlsx: Optional[_UvicornThread] = None
        self._svc_root = resource_path("client_app", "app", "services")
        self.services = {
            "nav_report": self._svc_root / "nav_report",
            "xlsx_merge": self._svc_root / "xlsx_merge",
        }

    @property
    def nav_base_url(self) -> str:
        return f"http://127.0.0.1:{self.nav_port}"

    @property
    def xlsx_base_url(self) -> str:
        return f"http://127.0.0.1:{self.xlsx_port}"

    def update_data_root(self, new_root: str) -> None:
        self.data_root = Path(new_root)

    def _config_path(self) -> Path:
        return self.data_root / "config" / "config.json"

    def _service_data_dir(self, name: str) -> Path:
        if name == "nav_report":
            return self.data_root / "6002" / "nav_report"
        if name == "xlsx_merge":
            return self.data_root / "6000" / "xlsx_merge"
        raise RuntimeError(f"unknown service: {name}")

    def _service_main_file(self, name: str) -> Path:
        svc_root = self.services[name]
        p1 = svc_root / "app" / "main.py"
        p2 = svc_root / "main.py"
        if p1.exists():
            return p1
        if p2.exists():
            return p2
        raise FileNotFoundError(
            f"未找到内置服务入口文件：{name}。已检查：{p1} 和 {p2}。请确认打包时包含了 client_app/app/services/{name} 目录。"
        )

    def _service_thread(self, name: str) -> Optional[_UvicornThread]:
        return self._nav if name == "nav_report" else self._xlsx

    def _set_service_thread(self, name: str, value: Optional[_UvicornThread]) -> None:
        if name == "nav_report":
            self._nav = value
        else:
            self._xlsx = value

    def _load_cfg(self) -> Dict[str, Any]:
        if self._config_path().exists():
            try:
                return json.loads(self._config_path().read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _write_env(self, name: str, env_map: Dict[str, Any]) -> Path:
        svc_data = self._service_data_dir(name)
        svc_data.mkdir(parents=True, exist_ok=True)
        env_path = svc_data / ".env"
        lines = []
        for k, v in env_map.items():
            if v is None:
                continue
            vv = str(v).replace("\n", " ").strip()
            lines.append(f"{k}={vv}")
        env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        os.environ.update({k: str(v) for k, v in env_map.items() if v is not None})
        os.environ["SERVICE_CONFIG"] = str(self._config_path())
        return env_path

    def _build_env_map(self, name: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
        if name == "nav_report":
            data_dir = self._service_data_dir(name)
            allowlist_codes = cfg.get("allowlist_codes")
            if not allowlist_codes:
                dbp = self.data_root / "config" / "config.db"
                if dbp.exists():
                    import sqlite3
                    with sqlite3.connect(dbp) as conn:
                        cur = conn.execute("SELECT code FROM product_allowlist WHERE enabled=1 ORDER BY code")
                        allowlist_codes = ",".join([r[0] for r in cur.fetchall()])
            return {
                "DATA_DIR": data_dir.as_posix(),
                "PORT": cfg.get("nav_port", self.nav_port),
                "IMAP_HOST": cfg.get("imap_host", "imap.qq.com"),
                "IMAP_USER": cfg.get("imap_user", ""),
                "IMAP_PASS": cfg.get("imap_pass", ""),
                "IMAP_FOLDER": "INBOX",
                "MAIL_LOOKBACK_DAYS": cfg.get("mail_lookback_days", cfg.get("lookback_days", 3)),
                "IMAP_FOLDERS_MODE": cfg.get("imap_folders_mode", cfg.get("folder_mode", "all")),
                "IMAP_FOLDERS": cfg.get("imap_folders", cfg.get("folders", "INBOX")),
                "IMAP_FOLDERS_BLACKLIST": cfg.get("imap_folders_blacklist", cfg.get("folders_blacklist", "")),
                "WECOM_PUSH_ENABLED": "1" if cfg.get("push_enabled", True) else "0",
                "WECOM_WEBHOOK_URL": cfg.get("wecom_webhook_url", ""),
                "PRODUCT_CODE_ALLOWLIST": allowlist_codes or "",
                "PUSH_SLOTS": cfg.get("push_slots", "14:00,16:00,16:46"),
            }
        data_dir = self._service_data_dir(name)
        return {
            "DATA_DIR": data_dir.as_posix(),
            "PORT": cfg.get("xlsx_port", self.xlsx_port),
        }

    def apply_config(self, cfg: Dict[str, Any]) -> None:
        if cfg.get("data_root"):
            self.update_data_root(str(cfg["data_root"]))
        self.nav_port = int(cfg.get("nav_port", self.nav_port))
        self.xlsx_port = int(cfg.get("xlsx_port", self.xlsx_port))
        self._write_env("nav_report", self._build_env_map("nav_report", cfg))
        self._write_env("xlsx_merge", self._build_env_map("xlsx_merge", cfg))

    def start_all(self) -> None:
        cfg = self._load_cfg()
        self.apply_config(cfg)
        self.start("nav_report")
        self.start("xlsx_merge")

    def start(self, name: str) -> None:
        cfg = self._load_cfg()
        self.apply_config(cfg)
        current = self._service_thread(name)
        if current and self._is_up(self._health_url(name)):
            return
        main_file = self._service_main_file(name)
        app = _load_fastapi_app_from_file(main_file, "app", module_name=f"{name}_{int(time.time()*1000)}")
        port = self.nav_port if name == "nav_report" else self.xlsx_port
        thr = _UvicornThread(app, "127.0.0.1", port, log_level="info")
        self._set_service_thread(name, thr)
        thr.start()
        self._wait_health(self._health_url(name), timeout=25)

    def stop(self, name: str) -> None:
        current = self._service_thread(name)
        if current:
            current.stop()
        self._set_service_thread(name, None)

    def stop_all(self) -> None:
        self.stop("nav_report")
        self.stop("xlsx_merge")

    def restart_with_env(self, name: str, env: Dict[str, Any]) -> None:
        self._write_env(name, env)
        self.stop(name)
        self.start(name)

    def status(self) -> Dict[str, str]:
        return {
            "nav_report": "UP" if self._is_up(self._health_url("nav_report")) else "DOWN",
            "xlsx_merge": "UP" if self._is_up(self._health_url("xlsx_merge")) else "DOWN",
        }

    def test_webhook(self, url: str) -> Tuple[bool, str]:
        try:
            r = requests.post(url, json={"msgtype": "markdown", "markdown": {"content": "【大可客户端】Webhook 测试 ✅"}}, timeout=10)
            if r.status_code != 200:
                return False, f"HTTP {r.status_code}"
            j = r.json()
            if int(j.get("errcode", -1)) == 0:
                return True, "ok"
            return False, str(j)
        except Exception as e:
            return False, str(e)

    def call_nav_preview(self, lookback_days: int = 3, target_val_date_only: bool = True) -> Dict[str, Any]:
        self.start("nav_report")
        payload = {
            "force": True,
            "strict": False,
            "push": False,
            "lookback_days": int(lookback_days),
            "target_val_date_only": bool(target_val_date_only),
        }
        return self.post_json(self.nav_base_url + "/api/fetch_backfill", payload, timeout=180)

    def call_nav_fetch_probe(self, push: bool = False) -> Dict[str, Any]:
        self.start("nav_report")
        payload = {
            "force": True,
            "strict": False,
            "push": bool(push),
            "lookback_days": 3,
            "target_val_date_only": False,
        }
        return self.post_json(self.nav_base_url + "/api/fetch_backfill", payload, timeout=120)

    def call_nav_process_slot(self, slot: str, push: bool, force: bool = True, date_str: str = "") -> Dict[str, Any]:
        self.start("nav_report")
        payload = {"slot": slot, "force": bool(force), "push": bool(push)}
        if date_str:
            payload["date_str"] = date_str
        return self.post_json(self.nav_base_url + "/api/process_slot", payload, timeout=300)

    def run_merge_job(
        self,
        file_paths: list[str],
        target_amount: float,
        date_str: str = "",
        output_path: str = "",
        progress_cb: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        self.start("xlsx_merge")
        handlers = []
        try:
            files = []
            for fp in file_paths:
                fh = open(fp, "rb")
                handlers.append(fh)
                files.append(("files[]", (Path(fp).name, fh, "application/octet-stream")))
            payload = {
                "target_amount": str(target_amount),
                "date_str": date_str or datetime.datetime.now().strftime("%Y%m%d"),
            }
            if progress_cb:
                progress_cb("任务提交中…")
            resp = requests.post(self.xlsx_base_url + "/api/merge_job", files=files, data=payload, timeout=120)
            resp.raise_for_status()
            info = resp.json()
            job_id = info.get("job_id")
            if not job_id:
                return {"ok": False, "error": f"missing job_id: {info}"}
            if progress_cb:
                progress_cb(f"任务已启动：job_id={job_id}")
            while True:
                status = self.get_json(self.xlsx_base_url + f"/api/job/{job_id}", timeout=30)
                stage = status.get("status")
                if stage == "done":
                    break
                if stage == "error":
                    return {"ok": False, "error": status.get("error") or "merge failed", "job_id": job_id}
                if progress_cb:
                    progress_cb(f"任务处理中：状态={stage}")
                time.sleep(1)
            blob = requests.get(self.xlsx_base_url + f"/api/download_blob/{job_id}", timeout=120)
            blob.raise_for_status()
            out_file = Path(output_path) if output_path else (self._service_data_dir("xlsx_merge") / "out" / f"持仓比例-{payload['date_str']}.xlsx")
            out_file.parent.mkdir(parents=True, exist_ok=True)
            out_file.write_bytes(blob.content)
            if progress_cb:
                progress_cb(f"合并完成 ✅ 输出：{out_file}")
            return {"ok": True, "job_id": job_id, "date": status.get("date") or payload["date_str"], "out_file": str(out_file)}
        except Exception as e:
            return {"ok": False, "error": str(e)}
        finally:
            for fh in handlers:
                try:
                    fh.close()
                except Exception:
                    pass

    def get_json(self, url: str, timeout: int = 20) -> Dict[str, Any]:
        try:
            r = requests.get(url, timeout=timeout)
            return r.json() if r.headers.get("content-type", "").startswith("application/json") else {"ok": False, "status_code": r.status_code, "body": r.text}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def post_json(self, url: str, payload: Dict[str, Any], timeout: int = 60) -> Dict[str, Any]:
        try:
            r = requests.post(url, json=payload, timeout=timeout)
            return r.json() if r.headers.get("content-type", "").startswith("application/json") else {"ok": False, "status_code": r.status_code, "body": r.text}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _health_url(self, name: str) -> str:
        return (self.nav_base_url if name == "nav_report" else self.xlsx_base_url) + "/health"

    def _is_up(self, url: str) -> bool:
        try:
            r = requests.get(url, timeout=2)
            return r.status_code == 200
        except Exception:
            return False

    def _wait_health(self, url: str, timeout: int = 20) -> None:
        start = time.time()
        while time.time() - start < timeout:
            if self._is_up(url):
                return
            time.sleep(0.4)
