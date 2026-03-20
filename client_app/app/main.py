import os
import sys
import requests
from PySide6 import QtWidgets, QtCore
from client_app.app.core.config_store import load_config, save_config, ensure_runtime_dirs
from client_app.app.core.service_runner import ServiceProc, python_exe

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(APP_DIR, '..'))
CONFIG_PATH = os.path.join(ROOT_DIR, 'config', 'config.json')
EXAMPLE_PATH = os.path.join(ROOT_DIR, 'config', 'config.example.json')


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('大可 客户端（6000+6002 本地版）')
        self.nav_proc = ServiceProc('nav_report')
        self.merge_proc = ServiceProc('xlsx_merge')

        if not os.path.exists(CONFIG_PATH) and os.path.exists(EXAMPLE_PATH):
            cfg = load_config(EXAMPLE_PATH)
            save_config(CONFIG_PATH, cfg)
        self.cfg = load_config(CONFIG_PATH)
        if not self.cfg.get('data_root', '').strip():
            self.cfg['data_root'] = r'C:\DakeClient\data'
            save_config(CONFIG_PATH, self.cfg)
        ensure_runtime_dirs(self.cfg['data_root'])

        self._build_ui()
        self._refresh_status()
        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self._refresh_status)
        self.timer.start(2000)

    def _build_ui(self):
        w = QtWidgets.QWidget()
        self.setCentralWidget(w)
        layout = QtWidgets.QVBoxLayout(w)

        row = QtWidgets.QHBoxLayout()
        self.txtData = QtWidgets.QLineEdit(self.cfg.get('data_root', ''))
        btnPick = QtWidgets.QPushButton('选择数据目录')
        btnPick.clicked.connect(self.on_pick_dir)
        row.addWidget(QtWidgets.QLabel('数据根目录'))
        row.addWidget(self.txtData, 1)
        row.addWidget(btnPick)
        layout.addLayout(row)

        g1 = QtWidgets.QGroupBox('邮箱(IMAP)配置')
        f1 = QtWidgets.QFormLayout(g1)
        self.txtImapHost = QtWidgets.QLineEdit(self.cfg['imap'].get('host', 'imap.qq.com'))
        self.txtImapUser = QtWidgets.QLineEdit(self.cfg['imap'].get('user', ''))
        self.txtImapPass = QtWidgets.QLineEdit(self.cfg['imap'].get('pass', ''))
        self.txtImapPass.setEchoMode(QtWidgets.QLineEdit.Password)
        self.spinLookback = QtWidgets.QSpinBox()
        self.spinLookback.setRange(1, 30)
        self.spinLookback.setValue(int(self.cfg['imap'].get('lookback_days', 3)))
        self.cmbFolderMode = QtWidgets.QComboBox()
        self.cmbFolderMode.addItems(['all', 'inbox', 'custom'])
        self.cmbFolderMode.setCurrentText(self.cfg['imap'].get('folder_mode', 'all'))
        self.txtFolders = QtWidgets.QLineEdit(','.join(self.cfg['imap'].get('folders', ['INBOX'])))
        self.txtBlacklist = QtWidgets.QLineEdit(','.join(self.cfg['imap'].get('blacklist_keywords', [])))
        f1.addRow('IMAP Host', self.txtImapHost)
        f1.addRow('IMAP User', self.txtImapUser)
        f1.addRow('IMAP Pass', self.txtImapPass)
        f1.addRow('lookback_days', self.spinLookback)
        f1.addRow('folder_mode', self.cmbFolderMode)
        f1.addRow('folders(逗号)', self.txtFolders)
        f1.addRow('folders_blacklist(逗号)', self.txtBlacklist)
        layout.addWidget(g1)

        g2 = QtWidgets.QGroupBox('企业微信推送配置（群机器人 Webhook）')
        f2 = QtWidgets.QFormLayout(g2)
        self.chkPush = QtWidgets.QCheckBox('启用推送')
        self.chkPush.setChecked(bool(self.cfg['wecom'].get('push_enabled', True)))
        self.txtWebhook = QtWidgets.QLineEdit(self.cfg['wecom'].get('webhook_url', ''))
        btnTestWebhook = QtWidgets.QPushButton('测试Webhook')
        btnTestWebhook.clicked.connect(self.on_test_webhook)
        r2 = QtWidgets.QHBoxLayout()
        r2.addWidget(self.txtWebhook, 1)
        r2.addWidget(btnTestWebhook)
        f2.addRow(self.chkPush)
        f2.addRow('Webhook URL', r2)
        layout.addWidget(g2)

        g3 = QtWidgets.QGroupBox('产品白名单（禁用后报表完全不出现）')
        v3 = QtWidgets.QVBoxLayout(g3)
        self.tbl = QtWidgets.QTableWidget(0, 3)
        self.tbl.setHorizontalHeaderLabels(['code', 'display_name', 'enabled'])
        self.tbl.horizontalHeader().setStretchLastSection(True)
        v3.addWidget(self.tbl)
        row3 = QtWidgets.QHBoxLayout()
        self.txtCode = QtWidgets.QLineEdit()
        self.txtCode.setPlaceholderText('例如 SXZ218')
        self.txtName = QtWidgets.QLineEdit()
        self.txtName.setPlaceholderText('例如 放8')
        btnAdd = QtWidgets.QPushButton('添加/更新')
        btnAdd.clicked.connect(self.on_add_product)
        btnDel = QtWidgets.QPushButton('删除')
        btnDel.clicked.connect(self.on_del_product)
        btnToggle = QtWidgets.QPushButton('启用/禁用')
        btnToggle.clicked.connect(self.on_toggle_product)
        row3.addWidget(self.txtCode)
        row3.addWidget(self.txtName)
        row3.addWidget(btnAdd)
        row3.addWidget(btnToggle)
        row3.addWidget(btnDel)
        v3.addLayout(row3)
        layout.addWidget(g3)

        g4 = QtWidgets.QGroupBox('服务控制（本地运行，不开网页）')
        v4 = QtWidgets.QVBoxLayout(g4)
        self.lblStatus = QtWidgets.QLabel('状态: -')
        v4.addWidget(self.lblStatus)
        row4 = QtWidgets.QHBoxLayout()
        btnStart = QtWidgets.QPushButton('启动服务')
        btnStart.clicked.connect(self.on_start_services)
        btnStop = QtWidgets.QPushButton('停止服务')
        btnStop.clicked.connect(self.on_stop_services)
        btnReload = QtWidgets.QPushButton('应用配置(热加载)')
        btnReload.clicked.connect(self.on_reload_services)
        row4.addWidget(btnStart)
        row4.addWidget(btnStop)
        row4.addWidget(btnReload)
        v4.addLayout(row4)

        row5 = QtWidgets.QHBoxLayout()
        self.txtDate = QtWidgets.QLineEdit()
        self.txtDate.setPlaceholderText('手动日期YYYYMMDD(可空)')
        btnSlot14 = QtWidgets.QPushButton('触发 14:00')
        btnSlot16 = QtWidgets.QPushButton('触发 16:00')
        btnFinal = QtWidgets.QPushButton('触发 16:46(最终)')
        btnSlot14.clicked.connect(lambda: self.on_trigger_slot('14:00'))
        btnSlot16.clicked.connect(lambda: self.on_trigger_slot('16:00'))
        btnFinal.clicked.connect(self.on_trigger_final)
        row5.addWidget(self.txtDate)
        row5.addWidget(btnSlot14)
        row5.addWidget(btnSlot16)
        row5.addWidget(btnFinal)
        v4.addLayout(row5)
        layout.addWidget(g4)

        self.out = QtWidgets.QPlainTextEdit()
        self.out.setReadOnly(True)
        layout.addWidget(self.out, 1)
        self._load_products_table()

    def log(self, text: str):
        self.out.appendPlainText(text)

    def _service_url(self, service: str) -> str:
        port = int(self.cfg['services'][service]['port'])
        return f'http://127.0.0.1:{port}'

    def _load_products_table(self):
        items = self.cfg.get('products', [])
        self.tbl.setRowCount(len(items))
        for i, it in enumerate(items):
            self.tbl.setItem(i, 0, QtWidgets.QTableWidgetItem(str(it.get('code', ''))))
            self.tbl.setItem(i, 1, QtWidgets.QTableWidgetItem(str(it.get('display_name', ''))))
            self.tbl.setItem(i, 2, QtWidgets.QTableWidgetItem(str(int(it.get('enabled', 1)))))

    def _save_cfg_from_ui(self):
        self.cfg['data_root'] = self.txtData.text().strip()
        self.cfg['imap']['host'] = self.txtImapHost.text().strip()
        self.cfg['imap']['user'] = self.txtImapUser.text().strip()
        self.cfg['imap']['pass'] = self.txtImapPass.text().strip()
        self.cfg['imap']['lookback_days'] = int(self.spinLookback.value())
        self.cfg['imap']['folder_mode'] = self.cmbFolderMode.currentText().strip()
        self.cfg['imap']['folders'] = [x.strip() for x in self.txtFolders.text().split(',') if x.strip()]
        self.cfg['imap']['blacklist_keywords'] = [x.strip() for x in self.txtBlacklist.text().split(',') if x.strip()]
        self.cfg['wecom']['push_enabled'] = bool(self.chkPush.isChecked())
        self.cfg['wecom']['webhook_url'] = self.txtWebhook.text().strip()
        save_config(CONFIG_PATH, self.cfg)
        ensure_runtime_dirs(self.cfg['data_root'])

    def on_pick_dir(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, '选择数据根目录', self.txtData.text() or r'C:\DakeClient\data')
        if d:
            self.txtData.setText(d)
            self._save_cfg_from_ui()
            self.log(f'已选择数据目录: {d}')

    def on_test_webhook(self):
        self._save_cfg_from_ui()
        url = self.cfg['wecom']['webhook_url']
        if not url:
            QtWidgets.QMessageBox.warning(self, '缺少Webhook', '请先填写企业微信机器人Webhook URL')
            return
        payload = {'msgtype': 'markdown', 'markdown': {'content': '【客户端测试】Webhook 通道 OK'}}
        try:
            r = requests.post(url, json=payload, timeout=10)
            self.log(f'Webhook resp: {r.status_code} {r.text}')
        except Exception as e:
            self.log(f'Webhook error: {e}')

    def on_add_product(self):
        code = self.txtCode.text().strip().upper()
        name = self.txtName.text().strip()
        if not code:
            return
        items = self.cfg.get('products', [])
        found = False
        for it in items:
            if str(it.get('code', '')).upper() == code:
                it['display_name'] = name or it.get('display_name', '')
                it['enabled'] = 1
                found = True
        if not found:
            items.append({'code': code, 'display_name': name or code, 'enabled': 1})
        self.cfg['products'] = items
        self._save_cfg_from_ui()
        self._load_products_table()
        self.log(f'已添加/更新产品: {code} {name}')

    def on_del_product(self):
        code = self.txtCode.text().strip().upper()
        if not code:
            return
        self.cfg['products'] = [it for it in self.cfg.get('products', []) if str(it.get('code', '')).upper() != code]
        self._save_cfg_from_ui()
        self._load_products_table()
        self.log(f'已删除产品: {code}')

    def on_toggle_product(self):
        code = self.txtCode.text().strip().upper()
        if not code:
            return
        for it in self.cfg.get('products', []):
            if str(it.get('code', '')).upper() == code:
                it['enabled'] = 0 if int(it.get('enabled', 1)) == 1 else 1
        self._save_cfg_from_ui()
        self._load_products_table()
        self.log(f'已切换启用状态: {code}')

    def on_start_services(self):
        self._save_cfg_from_ui()
        env = os.environ.copy()
        env['SERVICE_CONFIG'] = CONFIG_PATH
        env['PYTHONPATH'] = os.pathsep.join([ROOT_DIR, env.get('PYTHONPATH', '')]).strip(os.pathsep)
        nav_port = int(self.cfg['services']['nav_report']['port'])
        merge_port = int(self.cfg['services']['xlsx_merge']['port'])
        cmd_nav = [python_exe(), '-m', 'uvicorn', 'client_app.app.services.nav_report.main:app', '--host', '127.0.0.1', '--port', str(nav_port)]
        cmd_merge = [python_exe(), '-m', 'uvicorn', 'client_app.app.services.xlsx_merge.main:app', '--host', '127.0.0.1', '--port', str(merge_port)]
        self.nav_proc.start(cmd_nav, env)
        self.merge_proc.start(cmd_merge, env)
        self.log('服务已启动（本机 127.0.0.1）')

    def on_stop_services(self):
        self.nav_proc.stop()
        self.merge_proc.stop()
        self.log('服务已停止')

    def on_reload_services(self):
        self._save_cfg_from_ui()
        try:
            r = requests.post(f"{self._service_url('nav_report')}/api/reload_config", timeout=5)
            self.log(f'nav reload: {r.status_code} {r.text}')
        except Exception as e:
            self.log(f'nav reload error: {e}')
        try:
            r = requests.post(f"{self._service_url('xlsx_merge')}/api/reload_config", timeout=5)
            self.log(f'merge reload: {r.status_code} {r.text}')
        except Exception as e:
            self.log(f'merge reload error: {e}')

    def on_trigger_slot(self, slot: str):
        payload = {'slot': slot, 'force': True}
        date_str = self.txtDate.text().strip()
        if len(date_str) == 8:
            payload['date_str'] = date_str
        try:
            r = requests.post(f"{self._service_url('nav_report')}/api/process_slot", json=payload, timeout=120)
            self.log(f'process_slot {slot}: {r.status_code} {r.text}')
        except Exception as e:
            self.log(f'process_slot error: {e}')

    def on_trigger_final(self):
        payload = {'force': True}
        date_str = self.txtDate.text().strip()
        if len(date_str) == 8:
            payload['date_str'] = date_str
        try:
            r = requests.post(f"{self._service_url('nav_report')}/api/process_prev_trading", json=payload, timeout=180)
            self.log(f'process_prev_trading: {r.status_code} {r.text}')
        except Exception as e:
            self.log(f'process_prev_trading error: {e}')

    def _refresh_status(self):
        states = []
        for service in ('nav_report', 'xlsx_merge'):
            ok = False
            try:
                r = requests.get(f"{self._service_url(service)}/health", timeout=1)
                ok = r.status_code == 200
            except Exception:
                ok = False
            states.append(f"{service}={'OK' if ok else 'DOWN'}")
        self.lblStatus.setText('状态: ' + ' | '.join(states))


def main():
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    w.resize(1100, 780)
    w.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
