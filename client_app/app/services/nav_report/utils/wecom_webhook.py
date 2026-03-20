import base64
import hashlib
import json
import os
import requests
from typing import Optional, Tuple

def _split_webhook(webhook_or_key: str) -> Tuple[str, str]:
    s = (webhook_or_key or "").strip()
    if not s:
        return "", ""
    if s.startswith("http://") or s.startswith("https://"):
        key = ""
        if "key=" in s:
            key = s.split("key=", 1)[1].split("&", 1)[0]
        return s, key
    key = s
    return f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={key}", key

def send_markdown(webhook_or_key: str, content: str, timeout: int = 20) -> dict:
    send_url, _ = _split_webhook(webhook_or_key)
    if not send_url:
        return {"ok": False, "err": "empty webhook"}
    payload = {"msgtype": "markdown", "markdown": {"content": content}}
    r = requests.post(send_url, json=payload, timeout=timeout)
    try:
        return r.json()
    except Exception:
        return {"ok": False, "status": r.status_code, "text": r.text}

def send_image_b64(webhook_or_key: str, b64: str, timeout: int = 30) -> dict:
    send_url, _ = _split_webhook(webhook_or_key)
    if not send_url:
        return {"ok": False, "err": "empty webhook"}
    img_bytes = base64.b64decode(b64)
    md5 = hashlib.md5(img_bytes).hexdigest()
    payload = {"msgtype": "image", "image": {"base64": b64, "md5": md5}}
    r = requests.post(send_url, json=payload, timeout=timeout)
    try:
        return r.json()
    except Exception:
        return {"ok": False, "status": r.status_code, "text": r.text}

def _upload_media_file(webhook_or_key: str, filename: str, file_bytes: bytes, timeout: int = 60) -> dict:
    _, key = _split_webhook(webhook_or_key)
    if not key:
        return {"ok": False, "err": "missing key for upload_media"}
    url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key={key}&type=file"
    files = {"media": (filename, file_bytes, "application/octet-stream")}
    r = requests.post(url, files=files, timeout=timeout)
    try:
        return r.json()
    except Exception:
        return {"ok": False, "status": r.status_code, "text": r.text}

def send_file_b64(webhook_or_key: str, filename: str, b64: str, timeout: int = 60) -> dict:
    file_bytes = base64.b64decode(b64)
    up = _upload_media_file(webhook_or_key, filename, file_bytes, timeout=timeout)
    if not isinstance(up, dict) or up.get("errcode") != 0:
        return {"ok": False, "stage": "upload_media", "resp": up}
    media_id = up.get("media_id")
    send_url, _ = _split_webhook(webhook_or_key)
    payload = {"msgtype": "file", "file": {"media_id": media_id}}
    r = requests.post(send_url, json=payload, timeout=timeout)
    try:
        return r.json()
    except Exception:
        return {"ok": False, "stage": "send_file", "status": r.status_code, "text": r.text}
