# -*- coding: utf-8 -*-
import datetime, threading, time

class SlotScheduler:
    def __init__(self, data_dir: str, app_id: str, runner, store):
        self.data_dir = data_dir
        self.app_id = app_id
        self.runner = runner
        self.store = store
        self._stop = threading.Event()
        self._th = None

    def start(self):
        if self._th and self._th.is_alive():
            return
        self._stop.clear()
        self._th = threading.Thread(target=self._loop, daemon=True)
        self._th.start()

    def stop(self):
        self._stop.set()

    def _loop(self):
        # 每 20 秒轮询一次（轻量），命中整点即执行
        fired = set()  # (date, slot)
        while not self._stop.is_set():
            now = datetime.datetime.now()
            today = now.strftime("%Y%m%d")
            for slot in ["14:00", "16:00", "16:46"]:
                hh, mm = slot.split(":")
                if now.hour == int(hh) and now.minute == int(mm):
                    key = (today, slot)
                    if key not in fired:
                        fired.add(key)
                        self.run_slot_now(slot, push=True)
            time.sleep(20)

    def run_slot_now(self, slot: str, push: bool):
        # 统一走 nav_report 的 /api/process_slot
        try:
            resp = self.runner.call_nav_process_slot(slot=slot, push=push, force=True)
            ok = bool(resp.get("ok"))
            if not ok:
                return False, str(resp)
            return True, "ok"
        except Exception as e:
            return False, f"执行失败：{e}"
