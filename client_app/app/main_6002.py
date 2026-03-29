# -*- coding: utf-8 -*-
import datetime
import json
import os
import subprocess
import sys
import threading
import traceback
from pathlib import Path

import requests

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

from PySide6.QtCore import QObject, QTimer, Signal, Slot
from PySide6.QtGui import QAction, QIcon
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QStyle,
    QSystemTrayIcon,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QVBoxLayout,
    QWidget,
)

from client_app.app.core.config_store import ConfigStore
from client_app.app.core.service_runner import LocalServiceRunner
from client_app.app.core.i18n import tr

APP_TITLE = tr("app_6002_title")
DEFAULT_DATA_DIR = r"C:\DakeClient\data" if os.name == "nt" else str(Path.home() / "DakeClient" / "data")


def _append_crash_log(msg: str) -> None:
    try:
        log_dir = Path(DEFAULT_DATA_DIR) / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        with open(log_dir / "crash.log", "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass


def _global_excepthook(exc_type, exc_value, exc_tb):
    txt = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    _append_crash_log(txt)
    sys.__excepthook__(exc_type, exc_value, exc_tb)


sys.excepthook = _global_excepthook


def resource_path(rel: str) -> str:
    base = Path(getattr(sys, "_MEIPASS", REPO_ROOT))
    candidates = [base / rel, base / "client_app" / rel]
    for p in candidates:
        if p.exists():
            return str(p)
    return str(candidates[0])


def open_path(path: str) -> None:
    if os.name == "nt":
        os.startfile(path)  # type: ignore[attr-defined]
    elif sys.platform == "darwin":
        subprocess.Popen(["open", path])
    else:
        subprocess.Popen(["xdg-open", path])


class UiSignals(QObject):
    log = Signal(str)
    error = Signal(str)
    refresh_status = Signal()
    products_loaded = Signal(list)


class Main6002(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_TITLE)
        self.resize(1260, 900)

        self.store = ConfigStore(default_data_dir=DEFAULT_DATA_DIR, app_id="6002")
        self.cfg = self.store.load()
        self.runner = LocalServiceRunner(self.store.get_data_root(), app_id="6002")

        self._timers: list[QTimer] = []
        self._last_success_text = self.cfg.get("last_success_push", "--")
        self.signals = UiSignals()
        self.signals.log.connect(self._log_on_ui)
        self.signals.error.connect(self._show_error_on_ui)
        self.signals.refresh_status.connect(self._refresh_status_labels)
        self.signals.products_loaded.connect(self._apply_products_on_ui)

        self._build_ui()
        self._build_tray()
        self._load_cfg_to_ui()
        self._refresh_status_labels()

        QTimer.singleShot(400, self._boot_sequence)

    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)

        top = QHBoxLayout()
        self.lb_status = QLabel(f"{tr('label_status')}：{tr('status_unknown')}")
        self.lb_target = QLabel(f"{tr('label_target_date')}：--")
        self.lb_next = QLabel(f"{tr('label_next_slot')}：--")
        self.lb_last = QLabel(f"{tr('label_last_push')}：{self._last_success_text}")
        top.addWidget(self.lb_status)
        top.addStretch(1)
        top.addWidget(self.lb_target)
        top.addStretch(1)
        top.addWidget(self.lb_next)
        top.addStretch(1)
        top.addWidget(self.lb_last)
        layout.addLayout(top)

        box_data = QGroupBox(tr("group_data_dir"))
        data_layout = QHBoxLayout(box_data)
        self.ed_data_root = QLineEdit()
        self.btn_pick_dir = QPushButton(tr("btn_pick_dir"))
        self.btn_open_data = QPushButton(tr("btn_open_dir"))
        self.btn_pick_dir.clicked.connect(self._pick_dir)
        self.btn_open_data.clicked.connect(self._open_data_dir)
        data_layout.addWidget(self.ed_data_root, 1)
        data_layout.addWidget(self.btn_pick_dir)
        data_layout.addWidget(self.btn_open_data)
        layout.addWidget(box_data)

        box_imap = QGroupBox(tr("group_imap"))
        imap_form = QFormLayout(box_imap)
        self.ed_imap_host = QLineEdit()
        self.ed_imap_user = QLineEdit()
        self.ed_imap_pass = QLineEdit()
        self.ed_imap_pass.setEchoMode(QLineEdit.Password)
        self.ed_lookback = QLineEdit("3")
        self.ed_folder_mode = QLineEdit("all")
        self.ed_folders = QLineEdit()
        self.ed_blacklist = QLineEdit()
        imap_form.addRow(tr("label_imap_host"), self.ed_imap_host)
        imap_form.addRow(tr("label_imap_user"), self.ed_imap_user)
        imap_form.addRow(tr("label_imap_pass"), self.ed_imap_pass)
        imap_form.addRow(tr("label_lookback_days"), self.ed_lookback)
        imap_form.addRow(tr("label_folder_mode"), self.ed_folder_mode)
        imap_form.addRow(tr("label_folders"), self.ed_folders)
        imap_form.addRow(tr("label_blacklist"), self.ed_blacklist)
        layout.addWidget(box_imap)

        box_push = QGroupBox(tr("group_push"))
        push_form = QFormLayout(box_push)
        self.chk_push = QCheckBox(tr("label_push_enabled"))
        self.ed_webhook = QLineEdit()
        self.ed_slots = QLineEdit("14:00,16:00,16:46")
        row_webhook = QHBoxLayout()
        row_webhook.addWidget(self.ed_webhook, 1)
        self.btn_test_webhook = QPushButton(tr("btn_test_webhook"))
        self.btn_test_webhook.clicked.connect(self._test_webhook)
        row_webhook.addWidget(self.btn_test_webhook)
        push_form.addRow("", self.chk_push)
        push_form.addRow(tr("label_webhook"), row_webhook)
        push_form.addRow(tr("label_push_slots"), self.ed_slots)
        layout.addWidget(box_push)

        box_products = QGroupBox(tr("group_products"))
        prod_layout = QVBoxLayout(box_products)
        self.tbl_products = QTableWidget(0, 3)
        self.tbl_products.setHorizontalHeaderLabels([tr("label_product_code"), tr("label_product_name"), tr("label_product_state")])
        self.tbl_products.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        prod_layout.addWidget(self.tbl_products)
        prod_row = QHBoxLayout()
        self.ed_code = QLineEdit()
        self.ed_code.setPlaceholderText(tr("placeholder_product_code"))
        self.ed_display = QLineEdit()
        self.ed_display.setPlaceholderText(tr("placeholder_product_name"))
        self.btn_add_product = QPushButton(tr("btn_add_or_update"))
        self.btn_toggle_product = QPushButton(tr("btn_toggle"))
        self.btn_delete_product = QPushButton(tr("btn_delete"))
        self.btn_refresh_products = QPushButton(tr("btn_refresh_products"))
        self.btn_add_product.clicked.connect(self._add_or_update_product)
        self.btn_toggle_product.clicked.connect(self._toggle_product)
        self.btn_delete_product.clicked.connect(self._delete_product)
        self.btn_refresh_products.clicked.connect(self._load_products)
        for w in [self.ed_code, self.ed_display, self.btn_add_product, self.btn_toggle_product, self.btn_delete_product, self.btn_refresh_products]:
            prod_row.addWidget(w)
        prod_layout.addLayout(prod_row)
        layout.addWidget(box_products)

        box_ops = QGroupBox(tr("group_actions"))
        ops_layout = QHBoxLayout(box_ops)
        self.btn_save_apply = QPushButton(tr("btn_save_apply"))
        self.btn_selfcheck = QPushButton(tr("btn_selfcheck"))
        self.btn_preview = QPushButton(tr("btn_preview"))
        self.btn_slot_14 = QPushButton(tr("btn_slot_14"))
        self.btn_slot_16 = QPushButton(tr("btn_slot_16"))
        self.btn_slot_1646 = QPushButton(tr("btn_slot_1646"))
        self.btn_export_log = QPushButton(tr("btn_export_logs"))
        self.btn_save_apply.clicked.connect(self._save_and_apply)
        self.btn_selfcheck.clicked.connect(self._selfcheck)
        self.btn_preview.clicked.connect(self._preview_scan)
        self.btn_slot_14.clicked.connect(lambda: self._run_slot("14:00", push=True))
        self.btn_slot_16.clicked.connect(lambda: self._run_slot("16:00", push=True))
        self.btn_slot_1646.clicked.connect(lambda: self._run_slot("16:46", push=True))
        self.btn_export_log.clicked.connect(self._export_logs)
        for w in [self.btn_save_apply, self.btn_selfcheck, self.btn_preview, self.btn_slot_14, self.btn_slot_16, self.btn_slot_1646, self.btn_export_log]:
            ops_layout.addWidget(w)
        layout.addWidget(box_ops)

        box_logs = QGroupBox(tr("group_logs"))
        logs_layout = QVBoxLayout(box_logs)
        self.log = QPlainTextEdit()
        self.log.setReadOnly(True)
        logs_layout.addWidget(self.log)
        layout.addWidget(box_logs, 1)

    def _build_tray(self):
        self.tray = QSystemTrayIcon(self)
        icon = QIcon(resource_path("assets/tray.png"))
        if icon.isNull():
            icon = self.style().standardIcon(QStyle.SP_ComputerIcon)
        self.setWindowIcon(icon)
        self.tray.setIcon(icon)
        self.tray.setToolTip(APP_TITLE)

        menu = QMenu(self)
        act_show = QAction(tr("btn_open_window"), self)
        act_show.triggered.connect(self.showNormal)
        menu.addAction(act_show)

        act_self = QAction(tr("btn_selfcheck"), self)
        act_self.triggered.connect(self._selfcheck)
        menu.addAction(act_self)

        act_14 = QAction(tr("btn_slot_14"), self)
        act_14.triggered.connect(lambda: self._run_slot("14:00", push=True))
        menu.addAction(act_14)

        act_16 = QAction(tr("btn_slot_16"), self)
        act_16.triggered.connect(lambda: self._run_slot("16:00", push=True))
        menu.addAction(act_16)

        act_1646 = QAction(tr("btn_slot_1646"), self)
        act_1646.triggered.connect(lambda: self._run_slot("16:46", push=True))
        menu.addAction(act_1646)

        menu.addSeparator()
        act_quit = QAction(tr("btn_quit"), self)
        act_quit.triggered.connect(self._quit_app)
        menu.addAction(act_quit)

        self.tray.setContextMenu(menu)
        self.tray.show()

    def closeEvent(self, event):
        self.hide()
        self._log(tr("log_minimized"))
        event.ignore()

    def _log(self, msg: str):
        try:
            self.signals.log.emit(str(msg))
        except Exception:
            pass

    @Slot(str)
    def _log_on_ui(self, msg: str):
        now = datetime.datetime.now().strftime("%H:%M:%S")
        self.log.appendPlainText(f"[{now}] {msg}")

    @Slot(str)
    def _show_error_on_ui(self, msg: str):
        self._log_on_ui(f"错误: {msg}")

    @Slot(list)
    def _apply_products_on_ui(self, items: list):
        self.tbl_products.setRowCount(0)
        for item in items:
            row = self.tbl_products.rowCount()
            self.tbl_products.insertRow(row)
            self.tbl_products.setItem(row, 0, QTableWidgetItem(str(item.get("code", ""))))
            self.tbl_products.setItem(row, 1, QTableWidgetItem(str(item.get("display_name", ""))))
            self.tbl_products.setItem(row, 2, QTableWidgetItem("启用" if int(item.get("enabled", 1)) == 1 else "禁用"))

    def _pick_dir(self):
        d = QFileDialog.getExistingDirectory(self, "选择数据根目录", self.ed_data_root.text().strip() or DEFAULT_DATA_DIR)
        if d:
            self.ed_data_root.setText(d)

    def _open_data_dir(self):
        root = self.ed_data_root.text().strip() or self.store.get_data_root()
        Path(root).mkdir(parents=True, exist_ok=True)
        open_path(root)

    def _export_logs(self):
        log_dir = Path(self.store.get_paths().log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        default_path = log_dir / f"desktop_6002_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        file_path, _ = QFileDialog.getSaveFileName(self, tr("btn_export_logs"), str(default_path), "Text Files (*.txt)")
        if not file_path:
            return
        Path(file_path).write_text(self.log.toPlainText(), encoding="utf-8")
        self._log(f"{tr('log_exported')}：{file_path}")

    def _post_json(self, url: str, payload: dict, timeout: int = 120) -> dict:
        try:
            r = requests.post(url, json=payload, timeout=timeout)
            return r.json() if r.headers.get("content-type", "").startswith("application/json") else {"ok": False, "status_code": r.status_code, "body": r.text}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _load_cfg_to_ui(self):
        self.cfg = self.store.load()
        self.ed_data_root.setText(self.cfg.get("data_root", self.store.get_data_root()))
        self.ed_imap_host.setText(self.cfg.get("imap_host", "imap.qq.com"))
        self.ed_imap_user.setText(self.cfg.get("imap_user", ""))
        self.ed_imap_pass.setText(self.cfg.get("imap_pass", ""))
        self.ed_lookback.setText(str(self.cfg.get("mail_lookback_days", 3)))
        self.ed_folder_mode.setText(self.cfg.get("imap_folders_mode", "all"))
        self.ed_folders.setText(self.cfg.get("imap_folders", "INBOX"))
        self.ed_blacklist.setText(self.cfg.get("imap_folders_blacklist", "Deleted,Trash,Junk,Spam,垃圾,已删除,Drafts,草稿,Sent,已发送"))
        self.chk_push.setChecked(bool(self.cfg.get("push_enabled", True)))
        self.ed_webhook.setText(self.cfg.get("wecom_webhook_url", ""))
        self.ed_slots.setText(self.cfg.get("push_slots", "14:00,16:00,16:46"))
        self.lb_last.setText(f"{tr('label_last_push')}：{self.cfg.get('last_success_push', '--')}")

    def _save_ui_to_cfg(self):
        cfg = self.store.load()
        cfg.update(
            {
                "data_root": self.ed_data_root.text().strip() or DEFAULT_DATA_DIR,
                "imap_host": self.ed_imap_host.text().strip(),
                "imap_user": self.ed_imap_user.text().strip(),
                "imap_pass": self.ed_imap_pass.text().strip(),
                "mail_lookback_days": int(self.ed_lookback.text().strip() or "3"),
                "imap_folders_mode": self.ed_folder_mode.text().strip() or "all",
                "imap_folders": self.ed_folders.text().strip(),
                "imap_folders_blacklist": self.ed_blacklist.text().strip(),
                "wecom_webhook_url": self.ed_webhook.text().strip(),
                "push_enabled": bool(self.chk_push.isChecked()),
                "push_slots": self.ed_slots.text().strip() or "14:00,16:00,16:46",
            }
        )
        self.store.save(cfg)
        self.cfg = self.store.load()
        self.runner.update_data_root(self.store.get_data_root())

    def _refresh_status_labels(self):
        st = self.runner.status()
        nav = tr("status_ok") if st.get("nav_report") == "UP" else tr("status_bad")
        xlsx = tr("status_ok") if st.get("xlsx_merge") == "UP" else tr("status_bad")
        self.lb_status.setText(f"{tr('label_status')}：nav_report={nav} | xlsx_merge={xlsx}")
        self.lb_next.setText(f"{tr('label_next_slot')}：{self._calc_next_slot_text()}")
        target = self.runner.get_json(self.runner.nav_base_url + "/api/target_date")
        self.lb_target.setText(f"{tr('label_target_date')}：{target.get('target_date', '--')}")
        self.lb_last.setText(f"{tr('label_last_push')}：{self.cfg.get('last_success_push', self._last_success_text)}")

    def _calc_next_slot_text(self) -> str:
        slots = [x.strip() for x in (self.ed_slots.text().strip() or "14:00,16:00,16:46").split(",") if x.strip()]
        now = datetime.datetime.now()
        candidates = []
        for slot in slots:
            hh, mm = map(int, slot.split(":"))
            target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if target <= now:
                target += datetime.timedelta(days=1)
            candidates.append((target, slot))
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1] if candidates else "--"

    def _save_and_apply(self, startup: bool = False):
        try:
            self._save_ui_to_cfg()
            self.runner.apply_config(self.cfg)
            self.runner.start_all()
            self._refresh_status_labels()
            self._load_products()
            self._schedule_slots()
            self._log("配置已保存并生效（本地服务已重启）")
            if not startup:
                QMessageBox.information(self, tr("tip"), tr("log_saved"))
        except FileNotFoundError as e:
            self._log(f"内置服务文件缺失：{e}")
            self._refresh_status_labels()
            if not startup:
                QMessageBox.warning(self, tr("tip"), f"内置服务文件缺失：{e}")
        except Exception as e:
            self._log(f"配置保存失败：{e}")
            if not startup:
                QMessageBox.warning(self, tr("tip"), f"配置保存失败：{e}")

    def _selected_product(self) -> tuple[str, bool]:
        row = self.tbl_products.currentRow()
        if row < 0:
            code = self.ed_code.text().strip().upper()
            return code, True
        code_item = self.tbl_products.item(row, 0)
        state_item = self.tbl_products.item(row, 2)
        code = code_item.text().strip().upper() if code_item else ""
        enabled = (state_item.text().strip() == "启用") if state_item else True
        return code, enabled

    def _load_products(self):
        data = self.runner.get_json(self.runner.nav_base_url + "/api/products")
        items = data.get("items", []) if data.get("ok") else []
        if not data.get("ok"):
            self._log(f"读取产品列表失败：{data}")
            return
        self.signals.products_loaded.emit(items)

    def _add_or_update_product(self):
        code = self.ed_code.text().strip().upper()
        display = self.ed_display.text().strip() or code
        if not code:
            QMessageBox.warning(self, tr("tip"), tr("msg_need_product_code"))
            return
        data = self.runner.post_json(self.runner.nav_base_url + "/api/products/add", {"code": code, "display_name": display})
        if data.get("ok"):
            self._log(tr("log_product_ok"))
            self._load_products()
        else:
            self._log(f"{tr('log_product_fail')} {data}")

    def _toggle_product(self):
        code, enabled = self._selected_product()
        if not code:
            QMessageBox.warning(self, tr("tip"), tr("msg_pick_product_or_code"))
            return
        data = self.runner.post_json(self.runner.nav_base_url + "/api/products/toggle", {"code": code, "enabled": not enabled})
        if data.get("ok"):
            self._log(tr("log_product_ok"))
            self._load_products()
        else:
            self._log(f"{tr('log_product_fail')} {data}")

    def _delete_product(self):
        code, _ = self._selected_product()
        if not code:
            QMessageBox.warning(self, tr("tip"), tr("msg_pick_product_or_code"))
            return
        data = self.runner.post_json(self.runner.nav_base_url + "/api/products/delete", {"code": code})
        if data.get("ok"):
            self._log(tr("log_product_ok"))
            self._load_products()
        else:
            self._log(f"{tr('log_product_fail')} {data}")

    def _test_webhook(self):
        url = self.ed_webhook.text().strip()
        if not url:
            QMessageBox.warning(self, tr("tip"), tr("msg_fill_webhook"))
            return
        ok, msg = self.runner.test_webhook(url)
        if ok:
            self._log("Webhook 测试成功")
            QMessageBox.information(self, tr("tip"), "Webhook 测试成功")
        else:
            self._log(f"Webhook 测试失败：{msg}")
            QMessageBox.warning(self, tr("tip"), f"Webhook 测试失败：{msg}")

    def _boot_sequence(self):
        self._save_and_apply(startup=True)
        QTimer.singleShot(800, self._selfcheck)

    def _selfcheck(self):
        def worker():
            self._save_ui_to_cfg()
            self._log(tr("log_selfcheck_start"))
            health = self.runner.get_json(self.runner.nav_base_url + "/health")
            if not health.get("ok"):
                self._log(f"{tr('log_selfcheck_fail')} nav_report /health 不可用：{health}")
                self.signals.refresh_status.emit()
                return
            products = self.runner.get_json(self.runner.nav_base_url + "/api/products")
            if not products.get("ok"):
                self._log(f"{tr('log_selfcheck_fail')} /api/products 读取失败：{products}")
                self.signals.refresh_status.emit()
                return
            resp = self.runner.call_nav_process_slot(slot="14:00", push=False, force=True)
            if not resp.get("ok"):
                self._log(f"{tr('log_selfcheck_fail')} 试跑失败：{resp}")
            else:
                self._log(f"{tr('log_selfcheck_ok')} 目标日={resp.get('target_date', '--')}")
            self.signals.refresh_status.emit()
            self._load_products()

        threading.Thread(target=worker, daemon=True).start()

    def _preview_scan(self):
        def worker():
            try:
                self._save_ui_to_cfg()
                self._log(tr("log_preview_start"))
                resp = self.runner.call_nav_preview(lookback_days=int(self.ed_lookback.text().strip() or "3"), target_val_date_only=True)
                if not resp.get("ok"):
                    self._log(f"【预览】失败：{resp}")
                    return
                buckets = resp.get("val_date_buckets", {})
                missing = resp.get("missing_codes", [])
                keys = resp.get("product_keys", [])
                folders = resp.get("folders", [])
                self._log(
                    "【预览】命中邮件数={att} | 命中文件夹={folders} | 估值日分桶={buckets} | 命中产品={keys} | 缺失产品={missing}".format(
                        att=resp.get("attachments", 0),
                        folders=",".join(folders) if folders else tr("preview_none"),
                        buckets=json.dumps(buckets, ensure_ascii=False),
                        keys=",".join(keys) if keys else tr("preview_none"),
                        missing=",".join(missing) if missing else tr("preview_none"),
                    )
                )
                self.signals.refresh_status.emit()
            except FileNotFoundError as e:
                self._log(f"内置服务文件缺失：{e}")
            except Exception as e:
                self._log(f"【预览】失败：{e}")

        threading.Thread(target=worker, daemon=True).start()

    def _schedule_slots(self):
        for timer in self._timers:
            try:
                timer.stop()
            except Exception:
                pass
        self._timers = []
        slots = [x.strip() for x in (self.ed_slots.text().strip() or "14:00,16:00,16:46").split(",") if x.strip()]

        def arm(slot: str):
            now = datetime.datetime.now()
            hh, mm = map(int, slot.split(":"))
            target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if target <= now:
                target += datetime.timedelta(days=1)
            ms = max(int((target - now).total_seconds() * 1000), 1000)
            timer = QTimer(self)
            timer.setSingleShot(True)

            def tick():
                self._run_slot(slot, push=True, auto=True)
                arm(slot)

            timer.timeout.connect(tick)
            timer.start(ms)
            self._timers.append(timer)
            self._log(f"{tr('log_sched_set')} {slot}（下次触发：{target.strftime('%Y-%m-%d %H:%M:%S')}）")

        for slot in slots:
            arm(slot)
        self._refresh_status_labels()

    def _run_slot(self, slot: str, push: bool = True, auto: bool = False):
        def worker():
            try:
                self._save_ui_to_cfg()
                prefix = "【自动运行】" if auto else "【手动运行】"
                self._log(f"{prefix} {tr('log_autorun')} {slot}：开始抓取/生成/推送…")
                resp = self.runner.call_nav_process_slot(slot=slot, push=push, force=True)
                if not resp.get("ok"):
                    self._log(f"{prefix} {slot} 失败：{resp}")
                    return
                if resp.get("pushed"):
                    target_date = resp.get("target_date") or resp.get("date") or "--"
                    stamp = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} / {target_date}"
                    cfg = self.store.load()
                    cfg["last_success_push"] = stamp
                    self.store.save(cfg)
                    self.cfg = self.store.load()
                    self._last_success_text = stamp
                self._log(f"{prefix} {slot} 完成 ✅ {json.dumps(resp, ensure_ascii=False)[:500]}")
                self.signals.refresh_status.emit()
            except FileNotFoundError as e:
                self._log(f"内置服务文件缺失：{e}")
                self.signals.refresh_status.emit()
            except Exception as e:
                self._log(f"{slot} 执行失败：{e}")
                self.signals.refresh_status.emit()

        threading.Thread(target=worker, daemon=True).start()

    def _quit_app(self):
        self.runner.stop_all()
        QApplication.quit()


def main():
    app = QApplication(sys.argv)
    w = Main6002()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
