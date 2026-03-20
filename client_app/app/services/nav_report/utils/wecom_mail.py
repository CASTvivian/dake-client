import base64
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from email import message_from_bytes
from email.header import decode_header
from email.message import Message

QYAPI_BASE = os.getenv("WECOM_API_BASE", "https://qyapi.weixin.qq.com").rstrip("/")

CORP_ID = os.getenv("WECOM_CORP_ID", "").strip()
CORP_SECRET = os.getenv("WECOM_MAIL_SECRET", "").strip()
MAIL_USER = os.getenv("WECOM_MAIL_USER", "").strip()
MAIL_USER_PARAM = os.getenv("WECOM_MAIL_USER_PARAM", "user").strip() or "user"

_token: Optional[str] = None
_token_expire_at: float = 0.0


def _decode_mime_value(value: str) -> str:
    if not value:
        return ""
    try:
        parts = decode_header(value)
    except Exception:
        return (value or "").strip()
    out = []
    for chunk, enc in parts:
        if isinstance(chunk, bytes):
            try:
                out.append(chunk.decode(enc or "utf-8", errors="ignore"))
            except Exception:
                out.append(chunk.decode("utf-8", errors="ignore"))
        else:
            out.append(str(chunk))
    return "".join(out).strip()


class WecomMailError(RuntimeError):
    pass


def _get_token() -> str:
    global _token, _token_expire_at
    now = time.time()
    if _token and now < _token_expire_at - 60:
        return _token

    if not CORP_ID or not CORP_SECRET:
        raise WecomMailError("WECOM_CORP_ID / WECOM_MAIL_SECRET not set")

    url = f"{QYAPI_BASE}/cgi-bin/gettoken"
    resp = requests.get(url, params={"corpid": CORP_ID, "corpsecret": CORP_SECRET}, timeout=20)
    data = resp.json()
    if data.get("errcode") != 0:
        raise WecomMailError(f"gettoken failed: {data}")

    _token = data["access_token"]
    _token_expire_at = now + int(data.get("expires_in", 7200))
    return _token


def list_inbox_mails(limit: int = 50, **kwargs) -> List[Dict[str, Any]]:
    token = _get_token()

    url = f"{QYAPI_BASE}/cgi-bin/exmail/app/list_mails"
    params: Dict[str, Any] = {"access_token": token}

    if MAIL_USER:
        params[MAIL_USER_PARAM] = MAIL_USER

    params.update(kwargs)
    params.setdefault("limit", limit)

    resp = requests.get(url, params=params, timeout=30)
    data = resp.json()
    if data.get("errcode") != 0:
        raise WecomMailError(f"list_mails failed: {data}")

    return data.get("mail_list") or data.get("mails") or []


def get_mail_raw(mail_id: str) -> bytes:
    token = _get_token()
    url = f"{QYAPI_BASE}/cgi-bin/exmail/app/get_mail"
    params = {"access_token": token, "mailid": mail_id}
    resp = requests.get(url, params=params, timeout=60)
    data = resp.json()
    if data.get("errcode") != 0:
        raise WecomMailError(f"get_mail failed: {data}")

    mail_b64 = data.get("mail_data")
    if not mail_b64:
        raise WecomMailError(f"get_mail missing mail_data: {list(data.keys())}")

    return base64.b64decode(mail_b64)


def extract_attachments_from_eml(eml_bytes: bytes) -> Tuple[Dict[str, str], List[Tuple[str, bytes]]]:
    msg: Message = message_from_bytes(eml_bytes)

    headers = {
        "message_id": (msg.get("Message-ID") or "").strip(),
        "subject": _decode_mime_value(msg.get("Subject") or ""),
        "from": _decode_mime_value(msg.get("From") or ""),
        "date": (msg.get("Date") or "").strip(),
    }

    atts: List[Tuple[str, bytes]] = []
    if msg.is_multipart():
        for part in msg.walk():
            filename = _decode_mime_value(part.get_filename() or "")
            if not filename:
                continue
            payload = part.get_payload(decode=True)
            if payload is None:
                continue
            low = filename.lower()
            if low.endswith((".xlsx", ".xls", ".xlsm", ".zip")):
                atts.append((filename, payload))

    return headers, atts
