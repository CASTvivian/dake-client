# -*- coding: utf-8 -*-
import datetime
import json
import os
import subprocess
import sys
import threading
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

from PySide6.QtCore import QTimer
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
        self.lb_status = QLabel("运行状态：未知")
        self.lb_target = QLabel("今日目标估值日：--")
        self.lb_next = QLabel("下一次任务：--")
        self.lb_last = QLabel(f"上次成功推送：{self._last_success_text}")
        top.addWidget(self.lb_status)
        top.addStretch(1)
        top.addWidget(self.lb_target)
        top.addStretch(1)
        top.addWidget(self.lb_next)
        top.addStretch(1)
        top.addWidget(self.lb_last)
        layout.addLayout(top)

        box_data = QGroupBox("数据目录")
        data_layout = QHBoxLayout(box_data)
        self.ed_data_root = QLineEdit()
        self.btn_pick_dir = QPushButton("选择数据目录")
        self.btn_open_data = QPushButton("打开数据目录")
        self.btn_pick_dir.clicked.connect(self._pick_dir)
        self.btn_open_data.clicked.connect(self._open_data_dir)
        data_layout.addWidget(self.ed_data_root, 1)
        data_layout.addWidget(self.btn_pick_dir)
        data_layout.addWidget(self.btn_open_data)
        layout.addWidget(box_data)

        box_imap = QGroupBox("邮箱配置")
        imap_form = QFormLayout(box_imap)
        self.ed_imap_host = QLineEdit()
        self.ed_imap_user = QLineEdit()
        self.ed_imap_pass = QLineEdit()
        self.ed_imap_pass.setEchoMode(QLineEdit.Password)
        self.ed_lookback = QLineEdit("3")
        self.ed_folder_mode = QLineEdit("all")
        self.ed_folders = QLineEdit()
        self.ed_blacklist = QLineEdit()
        imap_form.addRow("邮箱服务器地址", self.ed_imap_host)
        imap_form.addRow("邮箱账号", self.ed_imap_user)
        imap_form.addRow("邮箱密码/授权码", self.ed_imap_pass)
        imap_form.addRow("回溯天数", self.ed_lookback)
        imap_form.addRow("扫描模式（全部 / 指定）", self.ed_folder_mode)
        imap_form.addRow("扫描文件夹（逗号）", self.ed_folders)
        imap_form.addRow("黑名单文件夹（逗号）", self.ed_blacklist)
        layout.addWidget(box_imap)

        box_push = QGroupBox("推送配置")
        push_form = QFormLayout(box_push)
        self.chk_push = QCheckBox("启用推送")
        self.ed_webhook = QLineEdit()
        self.ed_slots = QLineEdit("14:00,16:00,16:46")
        row_webhook = QHBoxLayout()
        row_webhook.addWidget(self.ed_webhook, 1)
        self.btn_test_webhook = QPushButton("测试 Webhook")
        self.btn_test_webhook.clicked.connect(self._test_webhook)
        row_webhook.addWidget(self.btn_test_webhook)
        push_form.addRow("", self.chk_push)
        push_form.addRow("企业微信机器人地址", row_webhook)
        push_form.addRow("推送时段", self.ed_slots)
        layout.addWidget(box_push)

        box_products = QGroupBox("产品白名单（禁用后报表完全不出现）")
        prod_layout = QVBoxLayout(box_products)
        self.tbl_products = QTableWidget(0, 3)
        self.tbl_products.setHorizontalHeaderLabels(["产品代码", "中文简称", "启用状态"])
        self.tbl_products.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        prod_layout.addWidget(self.tbl_products)
        prod_row = QHBoxLayout()
        self.ed_code = QLineEdit()
        self.ed_code.setPlaceholderText("例如 SXZ218")
        self.ed_display = QLineEdit()
        self.ed_display.setPlaceholderText("例如 放8")
        self.btn_add_product = QPushButton("添加/更新")
        self.btn_toggle_product = QPushButton("启用/禁用")
        self.btn_delete_product = QPushButton("删除")
        self.btn_refresh_products = QPushButton("刷新产品列表")
        self.btn_add_product.clicked.connect(self._add_or_update_product)
        self.btn_toggle_product.clicked.connect(self._toggle_product)
        self.btn_delete_product.clicked.connect(self._delete_product)
        self.btn_refresh_products.clicked.connect(self._load_products)
        for w in [self.ed_code, self.ed_display, self.btn_add_product, self.btn_toggle_product, self.btn_delete_product, self.btn_refresh_products]:
            prod_row.addWidget(w)
        prod_layout.addLayout(prod_row)
        layout.addWidget(box_products)

        box_ops = QGroupBox("操作区")
        ops_layout = QHBoxLayout(box_ops)
        self.btn_save_apply = QPushButton("保存并应用配置")
        self.btn_selfcheck = QPushButton("立即自检")
        self.btn_preview = QPushButton("扫描预览（不推送）")
        self.btn_slot_14 = QPushButton("触发 14:00")
        self.btn_slot_16 = QPushButton("触发 16:00")
        self.btn_slot_1646 = QPushButton("触发 16:46（最终）")
        self.btn_export_log = QPushButton("导出日志")
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

        box_logs = QGroupBox("日志区")
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
        act_show = QAction("打开主窗口", self)
        act_show.triggered.connect(self.showNormal)
        menu.addAction(act_show)

        act_self = QAction("立即自检", self)
        act_self.triggered.connect(self._selfcheck)
        menu.addAction(act_self)

        act_14 = QAction("触发 14:00", self)
        act_14.triggered.connect(lambda: self._run_slot("14:00", push=True))
        menu.addAction(act_14)

        act_16 = QAction("触发 16:00", self)
        act_16.triggered.connect(lambda: self._run_slot("16:00", push=True))
        menu.addAction(act_16)

        act_1646 = QAction("触发 16:46（最终）", self)
        act_1646.triggered.connect(lambda: self._run_slot("16:46", push=True))
        menu.addAction(act_1646)

        menu.addSeparator()
        act_quit = QAction("退出程序", self)
        act_quit.triggered.connect(self._quit_app)
        menu.addAction(act_quit)

        self.tray.setContextMenu(menu)
        self.tray.show()

    def closeEvent(self, event):
        self.hide()
        self._log("已最小化到托盘（程序仍在后台自动运行）")
        event.ignore()

    def _log(self, msg: str):
        now = datetime.datetime.now().strftime("%H:%M:%S")
        self.log.appendPlainText(f"[{now}] {msg}")

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
        file_path, _ = QFileDialog.getSaveFileName(self, "导出日志", str(default_path), "Text Files (*.txt)")
        if not file_path:
            return
        Path(file_path).write_text(self.log.toPlainText(), encoding="utf-8")
        self._log(f"日志已导出：{file_path}")

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
        self.lb_last.setText(f"上次成功推送：{self.cfg.get('last_success_push', '--')}")

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
        nav = "正常" if st.get("nav_report") == "UP" else "异常"
        xlsx = "正常" if st.get("xlsx_merge") == "UP" else "异常"
        self.lb_status.setText(f"运行状态：nav_report={nav} | xlsx_merge={xlsx}")
        self.lb_next.setText(f"下一次任务：{self._calc_next_slot_text()}")
        target = self.runner.get_json(self.runner.nav_base_url + "/api/target_date")
        self.lb_target.setText(f"今日目标估值日：{target.get('target_date', '--')}")
        self.lb_last.setText(f"上次成功推送：{self.cfg.get('last_success_push', self._last_success_text)}")

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
                QMessageBox.information(self, "提示", "配置已保存并生效。")
        except Exception as e:
            self._log(f"配置保存失败：{e}")
            if not startup:
                QMessageBox.warning(self, "提示", f"配置保存失败：{e}")

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
        self.tbl_products.setRowCount(0)
        items = data.get("items", []) if data.get("ok") else []
        if not data.get("ok"):
            self._log(f"读取产品列表失败：{data}")
            return
        for item in items:
            row = self.tbl_products.rowCount()
            self.tbl_products.insertRow(row)
            self.tbl_products.setItem(row, 0, QTableWidgetItem(str(item.get("code", ""))))
            self.tbl_products.setItem(row, 1, QTableWidgetItem(str(item.get("display_name", ""))))
            self.tbl_products.setItem(row, 2, QTableWidgetItem("启用" if int(item.get("enabled", 1)) == 1 else "禁用"))

    def _add_or_update_product(self):
        code = self.ed_code.text().strip().upper()
        display = self.ed_display.text().strip() or code
        if not code:
            QMessageBox.warning(self, "提示", "产品代码不能为空")
            return
        data = self.runner.post_json(self.runner.nav_base_url + "/api/products/add", {"code": code, "display_name": display})
        if data.get("ok"):
            self._log("产品操作成功 ✅")
            self._load_products()
        else:
            self._log(f"产品操作失败 ❌ {data}")

    def _toggle_product(self):
        code, enabled = self._selected_product()
        if not code:
            QMessageBox.warning(self, "提示", "请先选择产品或输入产品代码")
            return
        data = self.runner.post_json(self.runner.nav_base_url + "/api/products/toggle", {"code": code, "enabled": not enabled})
        if data.get("ok"):
            self._log("产品操作成功 ✅")
            self._load_products()
        else:
            self._log(f"产品操作失败 ❌ {data}")

    def _delete_product(self):
        code, _ = self._selected_product()
        if not code:
            QMessageBox.warning(self, "提示", "请先选择产品或输入产品代码")
            return
        data = self.runner.post_json(self.runner.nav_base_url + "/api/products/delete", {"code": code})
        if data.get("ok"):
            self._log("产品操作成功 ✅")
            self._load_products()
        else:
            self._log(f"产品操作失败 ❌ {data}")

    def _test_webhook(self):
        url = self.ed_webhook.text().strip()
        if not url:
            QMessageBox.warning(self, "提示", "请先填写企业微信机器人地址")
            return
        ok, msg = self.runner.test_webhook(url)
        if ok:
            self._log("Webhook 测试成功")
            QMessageBox.information(self, "提示", "Webhook 测试成功")
        else:
            self._log(f"Webhook 测试失败：{msg}")
            QMessageBox.warning(self, "提示", f"Webhook 测试失败：{msg}")

    def _boot_sequence(self):
        self._save_and_apply(startup=True)
        self._selfcheck()

    def _selfcheck(self):
        def worker():
            self._save_ui_to_cfg()
            self._log("【自检】开始：检查本地服务与配置…")
            health = self.runner.get_json(self.runner.nav_base_url + "/health")
            if not health.get("ok"):
                self._log(f"【自检】失败 ❌ nav_report /health 不可用：{health}")
                self._refresh_status_labels()
                return
            products = self.runner.get_json(self.runner.nav_base_url + "/api/products")
            if not products.get("ok"):
                self._log(f"【自检】失败 ❌ /api/products 读取失败：{products}")
                self._refresh_status_labels()
                return
            resp = self.runner.call_nav_process_slot(slot="14:00", push=False, force=True)
            if not resp.get("ok"):
                self._log(f"【自检】失败 ❌ 试跑失败：{resp}")
            else:
                self._log(f"【自检】完成 ✅（本次不推送） 目标日={resp.get('target_date', '--')}")
            self._refresh_status_labels()
            self._load_products()

        threading.Thread(target=worker, daemon=True).start()

    def _preview_scan(self):
        def worker():
            self._save_ui_to_cfg()
            self._log("【预览】开始扫描（不推送）…")
            payload = {
                "force": True,
                "strict": False,
                "push": False,
                "lookback_days": int(self.ed_lookback.text().strip() or "3"),
                "target_val_date_only": True,
            }
            resp = self.runner.post_json(self.runner.nav_base_url + "/api/fetch_backfill", payload, timeout=180)
            if not resp.get("ok"):
                self._log(f"【预览】失败：{resp}")
                return
            buckets = resp.get("val_date_buckets", {})
            missing = resp.get("missing_codes", [])
            keys = resp.get("product_keys", [])
            self._log(
                "【预览】命中邮件数={att} | 估值日分桶={buckets} | 命中产品={keys} | 缺失产品={missing}".format(
                    att=resp.get("attachments", 0),
                    buckets=json.dumps(buckets, ensure_ascii=False),
                    keys=",".join(keys) if keys else "无",
                    missing=",".join(missing) if missing else "无",
                )
            )
            self._refresh_status_labels()

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
            self._log(f"【定时】已设置 {slot}（下次触发：{target.strftime('%Y-%m-%d %H:%M:%S')}）")

        for slot in slots:
            arm(slot)
        self._refresh_status_labels()

    def _run_slot(self, slot: str, push: bool = True, auto: bool = False):
        def worker():
            self._save_ui_to_cfg()
            prefix = "【自动运行】" if auto else "【手动运行】"
            self._log(f"{prefix} 触发 {slot}：开始抓取/生成/推送…")
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
            self._refresh_status_labels()

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
