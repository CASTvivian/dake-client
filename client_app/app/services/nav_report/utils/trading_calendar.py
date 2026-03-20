import os, json, datetime
from pathlib import Path
import requests


def _ymd(d: datetime.date) -> str:
    return d.strftime("%Y%m%d")


def _parse_ymd(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y%m%d").date()


class TradingCalendarCN:
    def __init__(self, file_path: str, remote_url: str = "", auto_update: bool = True):
        self.file_path = Path(file_path)
        self.remote_url = (remote_url or "").strip()
        self.auto_update = auto_update
        self.days = set()

    def load(self):
        if self.file_path.exists():
            try:
                obj = json.loads(self.file_path.read_text(encoding="utf-8"))
                self.days = set(map(str, obj.get("days") or []))
            except Exception:
                self.days = set()
        else:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            self.file_path.write_text('{"days":[]}', encoding='utf-8')
            self.days = set()

    def try_update(self) -> bool:
        if not self.auto_update or not self.remote_url:
            return False
        try:
            r = requests.get(self.remote_url, timeout=10)
            r.raise_for_status()
            obj = r.json()
            days = [str(x) for x in (obj.get("days") or [])]
            for x in days[:10]:
                _parse_ymd(x)
            self.file_path.write_text(json.dumps({"days": days}, ensure_ascii=False), encoding='utf-8')
            self.days = set(days)
            return True
        except Exception:
            return False

    def is_trading_day(self, ymd: str) -> bool:
        return str(ymd) in self.days

    def prev_trading_day(self, ymd: str) -> str:
        d = _parse_ymd(str(ymd))
        for _ in range(370):
            d = d - datetime.timedelta(days=1)
            s = _ymd(d)
            if self.is_trading_day(s):
                return s
        return _ymd(d)


def build_calendar_from_env() -> TradingCalendarCN:
    fp = os.getenv("TRADING_CALENDAR_FILE", "").strip()
    if not fp:
        fp = "./data/calendar/trading_days_CN_A.json"
    remote = os.getenv("TRADING_CALENDAR_REMOTE_URL", "").strip()
    auto = os.getenv("TRADING_CALENDAR_AUTO_UPDATE", "1").strip() != "0"
    cal = TradingCalendarCN(fp, remote, auto)
    cal.load()
    return cal
