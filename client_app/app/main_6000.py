# -*- coding: utf-8 -*-
import datetime
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

from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QVBoxLayout,
    QWidget,
)

from client_app.app.core.config_store import ConfigStore
from client_app.app.core.service_runner import LocalServiceRunner
from client_app.app.core.i18n import tr

APP_TITLE = tr("app_6000_title")
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


class Main6000(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_TITLE)
        self.setWindowIcon(QIcon(resource_path("assets/tray.png")))
        self.resize(1120, 780)

        self.store = ConfigStore(default_data_dir=DEFAULT_DATA_DIR, app_id="6000")
        self.cfg = self.store.load()
        self.runner = LocalServiceRunner(self.store.get_data_root(), app_id="6000")
        self.selected_files: list[str] = []

        self._build_ui()
        self._load_cfg_to_ui()
        self._boot_sequence()

    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)

        top = QHBoxLayout()
        self.lb_status = QLabel(f"{tr('label_status')}：{tr('status_unknown')}")
        self.lb_output = QLabel(f"{tr('label_output_dir')}：--")
        top.addWidget(self.lb_status)
        top.addStretch(1)
        top.addWidget(self.lb_output)
        layout.addLayout(top)

        box_cfg = QGroupBox(tr("group_data_dir"))
        cfg_form = QFormLayout(box_cfg)
        row_dir = QHBoxLayout()
        self.ed_data_root = QLineEdit()
        self.btn_pick_dir = QPushButton(tr("btn_pick_dir"))
        self.btn_open_dir = QPushButton(tr("btn_open_output_dir"))
        self.btn_pick_dir.clicked.connect(self._pick_dir)
        self.btn_open_dir.clicked.connect(self._open_output_dir)
        row_dir.addWidget(self.ed_data_root)
        row_dir.addWidget(self.btn_pick_dir)
        row_dir.addWidget(self.btn_open_dir)
        cfg_form.addRow(tr("label_data_root"), row_dir)
        self.ed_target_amount = QLineEdit(str(self.cfg.get("merge_target_amount", 5000000)))
        cfg_form.addRow(tr("label_target_amount"), self.ed_target_amount)
        layout.addWidget(box_cfg)

        box_files = QGroupBox(tr("group_file_picker"))
        files_layout = QVBoxLayout(box_files)
        row_btns = QHBoxLayout()
        self.btn_select_files = QPushButton(tr("btn_select_files"))
        self.btn_clear_files = QPushButton(tr("btn_clear_files"))
        self.btn_merge = QPushButton(tr("btn_start_merge"))
        self.btn_select_files.clicked.connect(self._select_files)
        self.btn_clear_files.clicked.connect(self._clear_files)
        self.btn_merge.clicked.connect(self._start_merge)
        row_btns.addWidget(self.btn_select_files)
        row_btns.addWidget(self.btn_clear_files)
        row_btns.addStretch(1)
        row_btns.addWidget(self.btn_merge)
        files_layout.addLayout(row_btns)
        self.tbl_files = QTableWidget(0, 2)
        self.tbl_files.setHorizontalHeaderLabels(["文件名", "大小(KB)"])
        self.tbl_files.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        files_layout.addWidget(self.tbl_files)
        layout.addWidget(box_files)

        box_out = QGroupBox(tr("group_outputs"))
        out_layout = QVBoxLayout(box_out)
        self.list_outputs = QListWidget()
        self.list_outputs.itemDoubleClicked.connect(lambda item: open_path(item.text()))
        out_layout.addWidget(self.list_outputs)
        layout.addWidget(box_out)

        box_log = QGroupBox(tr("group_merge_logs"))
        log_layout = QVBoxLayout(box_log)
        self.log = QPlainTextEdit()
        self.log.setReadOnly(True)
        log_layout.addWidget(self.log)
        layout.addWidget(box_log, 1)

    def _log(self, msg: str):
        now = datetime.datetime.now().strftime("%H:%M:%S")
        self.log.appendPlainText(f"[{now}] {msg}")

    def _load_cfg_to_ui(self):
        self.cfg = self.store.load()
        self.ed_data_root.setText(self.cfg.get("data_root", self.store.get_data_root()))
        self.ed_target_amount.setText(str(self.cfg.get("merge_target_amount", 5000000)))
        self._refresh_status()
        self._refresh_outputs()

    def _save_cfg(self):
        cfg = self.store.load()
        cfg["data_root"] = self.ed_data_root.text().strip() or DEFAULT_DATA_DIR
        cfg["merge_target_amount"] = float(self.ed_target_amount.text().strip() or "5000000")
        self.store.save(cfg)
        self.cfg = self.store.load()
        self.runner.update_data_root(self.store.get_data_root())

    def _boot_sequence(self):
        self._save_cfg()
        self.runner.apply_config(self.cfg)
        self.runner.start("xlsx_merge")
        self._refresh_status()

    def _refresh_status(self):
        st = self.runner.status()
        merge = tr("status_ok") if st.get("xlsx_merge") == "UP" else tr("status_bad")
        self.lb_status.setText(f"{tr('label_status')}：xlsx_merge={merge}")
        self.lb_output.setText(f"{tr('label_output_dir')}：{self.store.get_paths().merge_out}")

    def _pick_dir(self):
        d = QFileDialog.getExistingDirectory(self, "选择数据根目录", self.ed_data_root.text().strip() or DEFAULT_DATA_DIR)
        if d:
            self.ed_data_root.setText(d)
            self._save_cfg()
            self.runner.apply_config(self.cfg)
            self.runner.start("xlsx_merge")
            self._refresh_status()
            self._refresh_outputs()

    def _open_output_dir(self):
        out_dir = Path(self.store.get_paths().merge_out)
        out_dir.mkdir(parents=True, exist_ok=True)
        open_path(str(out_dir))

    def _select_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "选择 Excel 文件", "", "Excel Files (*.xlsx *.xls)")
        if not files:
            return
        existing = set(self.selected_files)
        for fp in files:
            if fp not in existing:
                self.selected_files.append(fp)
        self._render_files()

    def _clear_files(self):
        self.selected_files = []
        self._render_files()

    def _render_files(self):
        self.tbl_files.setRowCount(0)
        for fp in self.selected_files:
            row = self.tbl_files.rowCount()
            self.tbl_files.insertRow(row)
            self.tbl_files.setItem(row, 0, QTableWidgetItem(Path(fp).name))
            size_kb = round(Path(fp).stat().st_size / 1024, 2) if Path(fp).exists() else 0
            self.tbl_files.setItem(row, 1, QTableWidgetItem(str(size_kb)))

    def _refresh_outputs(self):
        self.list_outputs.clear()
        out_dir = Path(self.store.get_paths().merge_out)
        out_dir.mkdir(parents=True, exist_ok=True)
        for fp in sorted(out_dir.glob("*.xlsx"), reverse=True):
            self.list_outputs.addItem(str(fp))

    def _start_merge(self):
        if not self.selected_files:
            QMessageBox.warning(self, tr("tip"), tr("msg_pick_excel"))
            return

        def worker():
            self._save_cfg()
            self.runner.apply_config(self.cfg)
            out_dir = Path(self.store.get_paths().merge_out)
            out_dir.mkdir(parents=True, exist_ok=True)
            date_str = datetime.datetime.now().strftime("%Y%m%d")
            out_file = out_dir / f"持仓比例-{date_str}.xlsx"
            result = self.runner.run_merge_job(
                file_paths=self.selected_files,
                target_amount=float(self.cfg.get("merge_target_amount", 5000000)),
                date_str=date_str,
                output_path=str(out_file),
                progress_cb=self._log,
            )
            if result.get("ok"):
                self._log(f"{tr('log_merge_done')} 输出：{result.get('out_file')}")
                self._refresh_outputs()
                self._refresh_status()
            else:
                self._log(f"{tr('log_merge_fail')}：{result.get('error')}")

        threading.Thread(target=worker, daemon=True).start()


def main():
    app = QApplication(sys.argv)
    w = Main6000()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
