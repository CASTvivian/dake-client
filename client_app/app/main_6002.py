# -*- coding: utf-8 -*-
import os, sys, time, json, threading, datetime
from pathlib import Path

# 兼容 `python client_app/app/main_6002.py` 直接启动。
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

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QFileDialog, QMessageBox, QCheckBox,
    QTableWidget, QTableWidgetItem, QHeaderView, QSystemTrayIcon, QMenu, QStyle
)
from PySide6.QtGui import QIcon, QAction
from PySide6.QtCore import Qt, QTimer

from client_app.app.core.config_store import ConfigStore
from client_app.app.core.service_runner import LocalServiceRunner
from client_app.app.core.scheduler import SlotScheduler
from client_app.app.core.i18n import tr
from client_app.app.core.selfcheck import run_selfcheck_once

APP_NAME = "大可客户端（6002 自动推送）"

DEFAULT_DATA_DIR = r"C:\DakeClient\data" if os.name == "nt" else str(Path.home() / "DakeClient" / "data")

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

class Main6002(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.resize(1100, 700)

        self.store = ConfigStore(default_data_dir=DEFAULT_DATA_DIR, app_id="6002")
        self.cfg = self.store.load()

        self.runner = LocalServiceRunner(self.store.data_dir, app_id="6002")
        self.scheduler = SlotScheduler(self.store.data_dir, app_id="6002", runner=self.runner, store=self.store)

        self._build_ui()
        self._build_tray()

        # 启动即加载配置 → 自检 → 试抓（不推送）→ 启动后台调度
        QTimer.singleShot(300, self._boot_sequence)

    def _build_ui(self):
        root = QWidget()
        layout = QVBoxLayout(root)

        # 数据目录
        row = QHBoxLayout()
        row.addWidget(QLabel(tr("数据根目录")))
        self.ed_data = QLineEdit(self.store.data_dir)
        self.ed_data.setReadOnly(True)
        btn_pick = QPushButton(tr("选择数据目录"))
        btn_pick.clicked.connect(self._pick_dir)
        row.addWidget(self.ed_data, 1)
        row.addWidget(btn_pick)
        layout.addLayout(row)

        # IMAP
        layout.addWidget(QLabel(tr("邮箱（IMAP）配置")))
        self.ed_host = self._line(layout, tr("IMAP Host"), self.cfg.get("imap_host","imap.qq.com"))
        self.ed_user = self._line(layout, tr("IMAP User"), self.cfg.get("imap_user",""))
        self.ed_pass = self._line(layout, tr("IMAP Pass"), self.cfg.get("imap_pass",""), password=True)

        self.ed_lookback = self._line(layout, tr("lookback_days"), str(self.cfg.get("lookback_days",3)))
        self.ed_folder_mode = self._line(layout, tr("folder_mode（all / list）"), self.cfg.get("folder_mode","all"))
        self.ed_folders = self._line(layout, tr("folders（逗号）"), ",".join(self.cfg.get("folders",["INBOX"])))
        self.ed_blacklist = self._line(layout, tr("folders_blacklist（逗号）"), self.cfg.get("folders_blacklist","Deleted,Trash,Junk,Spam,垃圾,已删除,Drafts,草稿,Sent,已发送"))

        # webhook
        layout.addWidget(QLabel(tr("企业微信推送配置（群机器人 Webhook）")))
        self.chk_push = QCheckBox(tr("启用推送"))
        self.chk_push.setChecked(bool(self.cfg.get("push_enabled", True)))
        layout.addWidget(self.chk_push)
        row2 = QHBoxLayout()
        row2.addWidget(QLabel(tr("Webhook URL")))
        self.ed_webhook = QLineEdit(self.cfg.get("wecom_webhook_url",""))
        btn_test = QPushButton(tr("测试Webhook"))
        btn_test.clicked.connect(self._test_webhook)
        row2.addWidget(self.ed_webhook, 1)
        row2.addWidget(btn_test)
        layout.addLayout(row2)

        # 产品白名单
        layout.addWidget(QLabel(tr("产品白名单（禁用后报表完全不出现）")))
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels([tr("code"), tr("display_name"), tr("enabled")])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table, 1)

        row3 = QHBoxLayout()
        self.ed_code = QLineEdit()
        self.ed_code.setPlaceholderText(tr("例如 SXZ218"))
        self.ed_disp = QLineEdit()
        self.ed_disp.setPlaceholderText(tr("例如 放8"))
        btn_add = QPushButton(tr("添加/更新"))
        btn_toggle = QPushButton(tr("启用/禁用"))
        btn_del = QPushButton(tr("删除"))
        btn_add.clicked.connect(self._add_or_update)
        btn_toggle.clicked.connect(self._toggle)
        btn_del.clicked.connect(self._delete)
        row3.addWidget(self.ed_code)
        row3.addWidget(self.ed_disp)
        row3.addWidget(btn_add)
        row3.addWidget(btn_toggle)
        row3.addWidget(btn_del)
        layout.addLayout(row3)

        # 服务状态 + 手工触发
        layout.addWidget(QLabel(tr("服务控制（后台运行，不开网页）")))
        self.lb_status = QLabel(tr("状态：nav_report=未知 | xlsx_merge=未知"))
        layout.addWidget(self.lb_status)

        row4 = QHBoxLayout()
        btn_apply = QPushButton(tr("应用配置（热加载）"))
        btn_apply.clicked.connect(self._apply_cfg)
        btn_manual_check = QPushButton(tr("立即自检+试抓（不推送）"))
        btn_manual_check.clicked.connect(self._selfcheck_and_probe)
        btn_14 = QPushButton(tr("触发 14:00"))
        btn_16 = QPushButton(tr("触发 16:00"))
        btn_1646 = QPushButton(tr("触发 16:46（最终）"))
        btn_14.clicked.connect(lambda: self._run_slot("14:00"))
        btn_16.clicked.connect(lambda: self._run_slot("16:00"))
        btn_1646.clicked.connect(lambda: self._run_slot("16:46"))
        row4.addWidget(btn_apply)
        row4.addWidget(btn_manual_check)
        row4.addStretch(1)
        row4.addWidget(btn_14)
        row4.addWidget(btn_16)
        row4.addWidget(btn_1646)
        layout.addLayout(row4)

        self.setCentralWidget(root)
        self._reload_products_table()

    def _line(self, layout, label, value, password=False):
        row = QHBoxLayout()
        row.addWidget(QLabel(label))
        ed = QLineEdit(value)
        if password:
            ed.setEchoMode(QLineEdit.Password)
        row.addWidget(ed, 1)
        layout.addLayout(row)
        return ed

    def _pick_dir(self):
        d = QFileDialog.getExistingDirectory(self, tr("选择数据目录"), self.store.data_dir)
        if d:
            self.store.set_data_dir(d)
            self.ed_data.setText(d)
            self.cfg = self.store.load()
            self._reload_products_table()
            QMessageBox.information(self, tr("提示"), tr("数据目录已切换。建议重启程序以确保服务目录正确。"))

    def closeEvent(self, event):
        # 点 X：隐藏到托盘，不退出
        event.ignore()
        self.hide()
        self.tray.showMessage(tr("大可客户端"), tr("已最小化到托盘，仍在后台运行。"), QSystemTrayIcon.Information, 2500)

    def _build_tray(self):
        self.tray = QSystemTrayIcon(self)
        tray_icon_path = resource_path("assets/tray.png")
        icon = QIcon(tray_icon_path)
        if icon.isNull():
            icon = self.style().standardIcon(QStyle.SP_ComputerIcon)
        self.setWindowIcon(icon)
        self.tray.setIcon(icon)
        menu = QMenu()

        act_show = QAction(tr("打开主窗口"), self)
        act_show.triggered.connect(self.showNormal)
        menu.addAction(act_show)

        menu.addSeparator()
        act_probe = QAction(tr("立即自检+试抓（不推送）"), self)
        act_probe.triggered.connect(self._selfcheck_and_probe)
        menu.addAction(act_probe)

        act_14 = QAction(tr("触发 14:00"), self); act_14.triggered.connect(lambda: self._run_slot("14:00"))
        act_16 = QAction(tr("触发 16:00"), self); act_16.triggered.connect(lambda: self._run_slot("16:00"))
        act_1646 = QAction(tr("触发 16:46（最终）"), self); act_1646.triggered.connect(lambda: self._run_slot("16:46"))
        menu.addAction(act_14); menu.addAction(act_16); menu.addAction(act_1646)

        menu.addSeparator()
        act_exit = QAction(tr("退出"), self)
        act_exit.triggered.connect(lambda: QApplication.quit())
        menu.addAction(act_exit)

        self.tray.setContextMenu(menu)
        self.tray.show()

    def _boot_sequence(self):
        # 1) 载入配置并热加载
        self._apply_cfg(silent=True)
        # 2) 启动本地服务
        self.runner.start_all()
        self._refresh_status()
        # 3) 自检 + 试抓（不推送）
        self._selfcheck_and_probe()
        # 4) 启动调度（14/16/16:46）
        self.scheduler.start()
        self.tray.showMessage(tr("大可客户端"), tr("后台调度已启动：14:00 / 16:00 / 16:46"), QSystemTrayIcon.Information, 2500)

    def _refresh_status(self):
        st = self.runner.status()
        self.lb_status.setText(tr(f"状态：nav_report={st.get('nav_report','?')} | xlsx_merge={st.get('xlsx_merge','?')}"))

    def _apply_cfg(self, silent=False):
        cfg = {
            "imap_host": self.ed_host.text().strip(),
            "imap_user": self.ed_user.text().strip(),
            "imap_pass": self.ed_pass.text(),
            "lookback_days": int(self.ed_lookback.text().strip() or "3"),
            "folder_mode": self.ed_folder_mode.text().strip() or "all",
            "folders": [x.strip() for x in self.ed_folders.text().split(",") if x.strip()],
            "folders_blacklist": self.ed_blacklist.text().strip(),
            "push_enabled": bool(self.chk_push.isChecked()),
            "wecom_webhook_url": self.ed_webhook.text().strip(),
        }
        self.store.save(cfg)
        self.cfg = self.store.load()
        # 热加载到 nav_report 的 .env / config（由 runner 实现）
        self.runner.apply_config(self.cfg)
        if not silent:
            QMessageBox.information(self, tr("提示"), tr("配置已保存并已应用。"))

    def _reload_products_table(self):
        items = self.store.products_list()
        self.table.setRowCount(0)
        for it in items:
            r = self.table.rowCount()
            self.table.insertRow(r)
            self.table.setItem(r, 0, QTableWidgetItem(it["code"]))
            self.table.setItem(r, 1, QTableWidgetItem(it["display_name"]))
            self.table.setItem(r, 2, QTableWidgetItem("1" if it["enabled"] else "0"))

    def _selected_code(self):
        row = self.table.currentRow()
        if row < 0:
            return ""
        item = self.table.item(row, 0)
        return item.text().strip() if item else ""

    def _add_or_update(self):
        code = self.ed_code.text().strip().upper()
        disp = self.ed_disp.text().strip()
        if not code:
            QMessageBox.warning(self, tr("提示"), tr("请填写产品 code。"))
            return
        self.store.products_upsert(code=code, display_name=disp or code, enabled=1)
        self._reload_products_table()

    def _toggle(self):
        code = self._selected_code()
        if not code:
            QMessageBox.warning(self, tr("提示"), tr("请先选择一行产品。"))
            return
        self.store.products_toggle(code)
        self._reload_products_table()

    def _delete(self):
        code = self._selected_code()
        if not code:
            QMessageBox.warning(self, tr("提示"), tr("请先选择一行产品。"))
            return
        self.store.products_delete(code)
        self._reload_products_table()

    def _test_webhook(self):
        ok, msg = self.runner.test_webhook(self.ed_webhook.text().strip())
        if ok:
            QMessageBox.information(self, tr("提示"), tr("Webhook 测试成功。"))
        else:
            QMessageBox.warning(self, tr("提示"), tr(f"Webhook 测试失败：{msg}"))

    def _selfcheck_and_probe(self):
        self._apply_cfg(silent=True)
        ok, report = run_selfcheck_once(self.runner, self.store)
        if ok:
            self.tray.showMessage(tr("大可客户端"), tr("自检通过：已完成试抓（不推送）"), QSystemTrayIcon.Information, 2500)
        else:
            self.tray.showMessage(tr("大可客户端"), tr("自检失败：请打开窗口查看配置"), QSystemTrayIcon.Warning, 2500)
            QMessageBox.warning(self, tr("提示"), report)

    def _run_slot(self, slot):
        self._apply_cfg(silent=True)
        ok, msg = self.scheduler.run_slot_now(slot, push=True)
        if ok:
            self.tray.showMessage(tr("大可客户端"), tr(f"已执行 {slot} 推送。"), QSystemTrayIcon.Information, 2500)
        else:
            QMessageBox.warning(self, tr("提示"), msg)

def main():
    app = QApplication(sys.argv)
    w = Main6002()
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
