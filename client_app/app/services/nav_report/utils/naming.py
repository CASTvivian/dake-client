import os, re

def product_key_from_filename(filename: str) -> str:
    base = os.path.basename(filename)
    base = re.sub(r"\.xlsx?$", "", base, flags=re.I)
    # 去掉日期前缀：2025-08-21_ 或 20250821_
    base = re.sub(r"^\d{4}[-]?\d{2}[-]?\d{2}_", "", base)
    # 去掉尾巴：估值表 / 估值表格 / 估值表v2 等常见后缀
    base = re.sub(r"(估值表.*)$", "", base)
    base = base.strip("_- ")
    return base

def yyyymmdd_dir(data_dir: str, ymd: str):
    yyyy, mm, dd = ymd[:4], ymd[4:6], ymd[6:8]
    raw_dir = os.path.join(data_dir, "raw", yyyy, mm, dd)
    out_dir = os.path.join(data_dir, "out", yyyy, mm, dd)
    return raw_dir, out_dir
