import json
from pathlib import Path
from PySide6 import QtWidgets, QtCore

RUNTIME_DIR = Path.cwd()
CONFIG_PATH = RUNTIME_DIR / "config" / "config.json"

DEFAULT_CFG = {
  "imap": {"host":"imap.qq.com","user":"","pass":"","folders_mode":"all","lookback_days":3},
  "wecom": {"webhook_url":"","push_enabled":True},
  "products": [
    {"code":"SBGZ87","display_name":"缙云","enabled":True},
    {"code":"STR134","display_name":"新力","enabled":True},
    {"code":"SXA927","display_name":"放10","enabled":True},
    {"code":"SXZ218","display_name":"放8","enabled":True}
  ]
}


def load_cfg():
    if CONFIG_PATH.exists():
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    return DEFAULT_CFG


def save_cfg(cfg):
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Dake Client")
        self.resize(820, 520)

        self.cfg = load_cfg()

        w = QtWidgets.QWidget()
        self.setCentralWidget(w)
        layout = QtWidgets.QVBoxLayout(w)

        form = QtWidgets.QFormLayout()
        self.imap_user = QtWidgets.QLineEdit(self.cfg["imap"].get("user", ""))
        self.imap_pass = QtWidgets.QLineEdit(self.cfg["imap"].get("pass", ""))
        self.imap_pass.setEchoMode(QtWidgets.QLineEdit.Password)
        self.webhook = QtWidgets.QLineEdit(self.cfg["wecom"].get("webhook_url", ""))
        self.push_enabled = QtWidgets.QCheckBox("启用推送")
        self.push_enabled.setChecked(bool(self.cfg["wecom"].get("push_enabled", True)))

        form.addRow("QQ/邮箱账号：", self.imap_user)
        form.addRow("IMAP 授权码：", self.imap_pass)
        form.addRow("企业微信机器人 Webhook：", self.webhook)
        form.addRow("", self.push_enabled)
        layout.addLayout(form)

        layout.addWidget(QtWidgets.QLabel("产品列表（code 唯一，不区分大小写；禁用后报表完全不出现）"))
        self.table = QtWidgets.QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Code", "简称", "启用"])
        self.table.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QtWidgets.QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(2, QtWidgets.QHeaderView.ResizeToContents)
        layout.addWidget(self.table)

        btn_row = QtWidgets.QHBoxLayout()
        self.btn_add = QtWidgets.QPushButton("添加产品")
        self.btn_del = QtWidgets.QPushButton("删除选中")
        self.btn_save = QtWidgets.QPushButton("保存并生效")
        btn_row.addWidget(self.btn_add)
        btn_row.addWidget(self.btn_del)
        btn_row.addStretch(1)
        btn_row.addWidget(self.btn_save)
        layout.addLayout(btn_row)

        self.btn_add.clicked.connect(self.add_row)
        self.btn_del.clicked.connect(self.del_selected)
        self.btn_save.clicked.connect(self.save_all)

        self.reload_table()

    def reload_table(self):
        items = self.cfg.get("products", [])
        self.table.setRowCount(0)
        for p in items:
            self._append_product(p)

    def _append_product(self, p):
        r = self.table.rowCount()
        self.table.insertRow(r)
        code = QtWidgets.QTableWidgetItem(str(p.get("code", "")).upper().strip())
        name = QtWidgets.QTableWidgetItem(str(p.get("display_name", "")).strip())
        chk = QtWidgets.QTableWidgetItem()
        chk.setFlags(chk.flags() | QtCore.Qt.ItemIsUserCheckable)
        chk.setCheckState(QtCore.Qt.Checked if p.get("enabled", True) else QtCore.Qt.Unchecked)

        self.table.setItem(r, 0, code)
        self.table.setItem(r, 1, name)
        self.table.setItem(r, 2, chk)

    def add_row(self):
        self._append_product({"code": "", "display_name": "", "enabled": True})

    def del_selected(self):
        rows = sorted(set(i.row() for i in self.table.selectedIndexes()), reverse=True)
        for r in rows:
            self.table.removeRow(r)

    def save_all(self):
        self.cfg["imap"]["user"] = self.imap_user.text().strip()
        self.cfg["imap"]["pass"] = self.imap_pass.text().strip()
        self.cfg["wecom"]["webhook_url"] = self.webhook.text().strip()
        self.cfg["wecom"]["push_enabled"] = bool(self.push_enabled.isChecked())

        products = []
        for r in range(self.table.rowCount()):
            code = (self.table.item(r, 0).text() if self.table.item(r, 0) else "").upper().strip()
            name = (self.table.item(r, 1).text() if self.table.item(r, 1) else "").strip()
            enabled = (self.table.item(r, 2).checkState() == QtCore.Qt.Checked) if self.table.item(r, 2) else True
            if not code:
                continue
            products.append({"code": code, "display_name": name or code, "enabled": enabled})

        self.cfg["products"] = products
        save_cfg(self.cfg)
        QtWidgets.QMessageBox.information(self, "已保存", f"已写入配置：\n{CONFIG_PATH}")


def main():
    app = QtWidgets.QApplication([])
    win = MainWindow()
    win.show()
    app.exec()


if __name__ == "__main__":
    main()
