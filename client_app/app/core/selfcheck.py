# -*- coding: utf-8 -*-
import json

def run_selfcheck_once(runner, store):
    # 1) 服务能起
    runner.start_all()
    st = runner.status()
    if st.get("nav_report") != "UP":
        return False, "nav_report 服务未启动。请检查端口占用或程序权限。"

    # 2) 配置基本项
    cfg = store.load()
    if not cfg.get("imap_user") or not cfg.get("imap_pass"):
        return False, "IMAP 账号或密码为空。请填写后再试。"
    if cfg.get("push_enabled") and not cfg.get("wecom_webhook_url"):
        return False, "已启用推送但 Webhook URL 为空。"

    # 3) 试抓一次（不推送）
    try:
        j = runner.call_nav_fetch_probe(push=False)
        # probe 只要返回 ok 就行
        if not j.get("ok", False):
            return False, f"试抓失败：{j}"
    except Exception as e:
        return False, f"试抓异常：{e}"

    return True, "ok"
