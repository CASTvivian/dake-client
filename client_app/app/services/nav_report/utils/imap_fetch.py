import datetime, re
from typing import List, Dict, Any, Optional
from imapclient import IMAPClient
import pyzmail

def fetch_attachments(
    host: str, port: int, user: str, password: str, folder: str,
    since_hours: int, unseen_only: bool,
    from_filter: Optional[str],
    subject_keyword: Optional[str],
    subject_regex: Optional[str],
    ext: str = ".xlsx"
) -> List[Dict[str, Any]]:
    since_dt = datetime.datetime.now() - datetime.timedelta(hours=since_hours)

    rgx = re.compile(subject_regex) if subject_regex else None

    with IMAPClient(host, port=port, ssl=True) as server:
        server.login(user, password)
        server.select_folder(folder)

        criteria = []
        if unseen_only:
            criteria.append("UNSEEN")
        criteria += ["SINCE", since_dt.date()]
        if from_filter:
            criteria += ["FROM", from_filter]
        if subject_keyword:
            criteria += ["SUBJECT", subject_keyword]

        uids = server.search(criteria)
        res = []

        fetched = server.fetch(uids, ["RFC822", "INTERNALDATE", "ENVELOPE"])
        for uid, msg_data in fetched.items():
            msg = pyzmail.PyzMessage.factory(msg_data[b"RFC822"])
            subject = msg.get_subject() or ""
            mail_from = msg.get_addresses("from")
            from_text = ",".join([a[1] for a in mail_from]) if mail_from else ""
            internal_date = msg_data.get(b"INTERNALDATE")
            received_at = internal_date.isoformat() if internal_date else ""

            # regex 进一步过滤
            if rgx and (not rgx.search(subject)):
                continue

            for part in msg.mailparts:
                fn = part.filename
                if not fn:
                    continue
                if not fn.lower().endswith(ext):
                    continue
                content = part.get_payload()
                res.append({
                    "uid": str(uid),
                    "subject": subject,
                    "from": from_text,
                    "received_at": received_at,
                    "filename": fn,
                    "content": content,
                })
        return res
