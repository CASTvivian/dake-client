import time, requests

def get_token(corp_id: str, secret: str) -> str:
    r = requests.get("https://qyapi.weixin.qq.com/cgi-bin/gettoken",
                     params={"corpid":corp_id,"corpsecret":secret}, timeout=10)
    r.raise_for_status()
    j = r.json()
    if j.get("errcode") != 0:
        raise RuntimeError(f"gettoken failed: {j}")
    return j["access_token"]

def upload_media(token: str, media_type: str, filename: str, data: bytes) -> str:
    url = "https://qyapi.weixin.qq.com/cgi-bin/media/upload"
    files = {"media": (filename, data)}
    r = requests.post(url, params={"access_token":token,"type":media_type}, files=files, timeout=30)
    r.raise_for_status()
    j = r.json()
    if j.get("errcode") != 0:
        raise RuntimeError(f"upload failed: {j}")
    return j["media_id"]

def send_chat_file(token: str, chatid: str, media_id: str):
    url = "https://qyapi.weixin.qq.com/cgi-bin/appchat/send"
    payload = {"chatid":chatid,"msgtype":"file","file":{"media_id":media_id}}
    r = requests.post(url, params={"access_token":token}, json=payload, timeout=10)
    r.raise_for_status()
    j = r.json()
    if j.get("errcode") != 0:
        raise RuntimeError(f"send file failed: {j}")

def send_chat_image(token: str, chatid: str, media_id: str):
    url = "https://qyapi.weixin.qq.com/cgi-bin/appchat/send"
    payload = {"chatid":chatid,"msgtype":"image","image":{"media_id":media_id}}
    r = requests.post(url, params={"access_token":token}, json=payload, timeout=10)
    r.raise_for_status()
    j = r.json()
    if j.get("errcode") != 0:
        raise RuntimeError(f"send image failed: {j}")

def send_chat_text(token: str, chatid: str, content: str):
    url = "https://qyapi.weixin.qq.com/cgi-bin/appchat/send"
    payload = {"chatid":chatid,"msgtype":"text","text":{"content":content}}
    r = requests.post(url, params={"access_token":token}, json=payload, timeout=10)
    r.raise_for_status()
    j = r.json()
    if j.get("errcode") != 0:
        raise RuntimeError(f"send text failed: {j}")
