import io, math, datetime, re
from typing import List, Dict, Any, Tuple, Optional
from PIL import Image, ImageDraw, ImageFont
import xlsxwriter

def fmt_pct(x):
    if x is None:
        return "--"
    try:
        if isinstance(x,str):
            t=x.strip()
            if t in ("--",""):
                return "--"
            x=float(t)
        # x 按比例小数处理
        return f"{x*100:.2f}%"
    except Exception:
        return "--"
    try: return f"{x*100:.2f}%"
    except: return "--"

def fmt_num(x, nd=4):
    if x is None: return "--"
    try:
        return f"{float(x):.{nd}f}"
    except:
        return "--"

PCT_FMT_COLUMNS = {"今年来收益","当日涨幅","当周涨幅","当月涨幅"}


def _pct_style_up_red_down_green(v):
    """PNG only: positive=red, negative=green, blank/zero=default."""
    try:
        if v is None:
            return (None, None)
        if isinstance(v, str):
            t = v.strip()
            if t in ('', '--'):
                return (None, None)
            if t.endswith('%'):
                v = float(t[:-1].strip()) / 100.0
            else:
                v = float(t)
        v = float(v)
    except Exception:
        return (None, None)
    if v > 0:
        return ((255, 235, 238), (198, 40, 40))
    if v < 0:
        return ((232, 245, 233), (46, 125, 50))
    return (None, None)

def build_excel(path: str, date_yyyymmdd: str, rows: List[Dict[str, Any]]):
    dt = datetime.datetime.strptime(date_yyyymmdd, "%Y%m%d")
    col_nav = f"{dt.month}.{dt.day}净值"

    wb = xlsxwriter.Workbook(path)
    ws = wb.add_worksheet("净值汇总")

    fmt_head = wb.add_format({"bold": True, "border":1, "align":"center", "valign":"vcenter"})
    fmt_txt  = wb.add_format({"border":1, "align":"left", "valign":"vcenter"})
    fmt_num4 = wb.add_format({"border":1, "align":"right", "valign":"vcenter", "num_format":"0.0000"})
    fmt_pct2 = wb.add_format({"border":1, "align":"right", "valign":"vcenter", "num_format":"0.00%"})
    fmt_pct_pos = wb.add_format({"border":1, "align":"right", "valign":"vcenter", "num_format":"0.00%", "font_color":"#1f7a1f"})
    fmt_pct_neg = wb.add_format({"border":1, "align":"right", "valign":"vcenter", "num_format":"0.00%", "font_color":"#c62828"})

    headers = ["产品名称", col_nav, "今年来收益", "当日涨幅", "当周涨幅", "当月涨幅", "备注"]
    ws.write_row(0, 0, headers, fmt_head)

    ws.set_column(0, 0, 40)
    ws.set_column(1, 1, 14)
    ws.set_column(2, 5, 12)
    ws.set_column(6, 6, 28)

    for i, r in enumerate(rows, start=1):
        ws.write(i, 0, r.get("product_key",""), fmt_txt)

        nav = r.get("nav")
        if isinstance(nav, (int,float)):
            ws.write_number(i, 1, float(nav), fmt_num4)
        else:
            ws.write(i, 1, "--", fmt_num4)

        for j, key in enumerate(["ytd_pct","day_pct","week_pct","month_pct"], start=2):
            v = r.get(key)
            if isinstance(v, (int,float)):
                fmt = fmt_pct_pos if v >= 0 else fmt_pct_neg
                ws.write_number(i, j, float(v), fmt)
            else:
                ws.write(i, j, "--", fmt_pct2)

        note = r.get("note") or r.get("parse_error") or ""
        ws.write(i, 6, str(note), fmt_txt)

    wb.close()

def _load_font(size: int):
    # 服务器渲染图片必须有中文字体，否则会变成方块
    candidates = [
        # Noto / 思源（CJK）
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSerifCJK-Bold.ttc",

        # 文泉驿
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",

        # 兜底
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for name in candidates:
        try:
            return ImageFont.truetype(name, size=size)
        except Exception:
            continue
    return ImageFont.load_default()



def render_table_image(path: str, title: str, headers: List[str], rows: List[List[str]], highlight_cols: List[int]=[]):
    # 金融表格图片渲染（两行换行 + 表头不裁切版）
    font_title = _load_font(30)
    font_head  = _load_font(20)
    font_cell  = _load_font(18)

    pad_x = 22
    pad_y = 18

    title_h = 64          # 标题区高度（加大，避免裁切）
    header_h = 46         # 表头行高（加大）
    base_row_h = 38
    max_lines_name = 2    # A：产品名最多两行

    def text_w(font, txt: str) -> int:
        try:
            return int(font.getlength(txt))
        except Exception:
            box = font.getbbox(txt)
            return int(box[2] - box[0])

    def wrap_text(font, txt: str, max_width: int, max_lines: int):
        txt = (txt or "").strip()
        if txt == "":
            return ["--"]
        lines, cur = [], ""
        for ch in txt:
            trial = cur + ch
            if text_w(font, trial) <= max_width or cur == "":
                cur = trial
            else:
                lines.append(cur)
                cur = ch
                if len(lines) >= max_lines:
                    break
        if len(lines) < max_lines and cur:
            lines.append(cur)

        joined = "".join(lines)
        if len(joined) < len(txt):
            last = lines[-1]
            while last and text_w(font, last + "…") > max_width:
                last = last[:-1]
            lines[-1] = (last + "…") if last else "…"
        return lines

    # 列宽策略：表头也要参与宽度计算（避免“今年来/当日/当周/当月”被裁）
    name_col_min = 380
    name_col_max = 560
    other_min = 110
    other_max = 190

    col_w = [name_col_min] + [other_min]*(len(headers)-1)

    # 非名称列：取 max(表头宽度, 内容宽度) + padding
    for c in range(1, len(headers)):
        w = max(text_w(font_head, headers[c]) + 34, other_min)
        for r in rows:
            w = max(w, text_w(font_cell, str(r[c])) + 26)
        col_w[c] = min(other_max, max(other_min, w))

    # 名称列：按最长产品名测宽，但不超过上限；也考虑表头宽度
    longest = headers[0]
    for r in rows:
        if r and r[0] and text_w(font_cell, r[0]) > text_w(font_cell, longest):
            longest = r[0]
    want = max(text_w(font_head, headers[0]) + 34, text_w(font_cell, longest) + 26)
    col_w[0] = min(name_col_max, max(name_col_min, want))

    # 行高自适应
    wrapped_name, row_h_list = [], []
    for r in rows:
        lines0 = wrap_text(font_cell, r[0], col_w[0]-18, max_lines_name)
        wrapped_name.append(lines0)
        rh = base_row_h * max(1, len(lines0))
        row_h_list.append(rh)

    W = pad_x*2 + sum(col_w)
    H = pad_y*2 + title_h + header_h + sum(row_h_list)

    img = Image.new("RGB", (W, H), "white")
    d = ImageDraw.Draw(img)

    # title（不裁切：放在 pad_y 内）
    d.text((pad_x, pad_y), title, fill=(0,0,0), font=font_title)

    top = pad_y + title_h

    # header
    x, y = pad_x, top
    for c, h in enumerate(headers):
        d.rectangle([x, y, x+col_w[c], y+header_h], fill=(245,245,245), outline=(210,210,210))
        if c == 0:
            # 产品名称表头左对齐
            d.text((x+10, y+12), h, fill=(0,0,0), font=font_head)
        else:
            # 短字段表头居中，避免看起来挤
            tw = text_w(font_head, h)
            d.text((x + (col_w[c]-tw)//2, y+12), h, fill=(0,0,0), font=font_head)
        x += col_w[c]

    # rows
    y += header_h
    for idx, r in enumerate(rows):
        rh = row_h_list[idx]
        x = pad_x
        for c, text in enumerate(r):
            t = str(text) if text is not None else "--"
            fill_color = (255,255,255)
            color = (0,0,0)
            if c in highlight_cols:
                fill_rgb, text_rgb = _pct_style_up_red_down_green(t)
                if fill_rgb is not None:
                    fill_color = fill_rgb
                if text_rgb is not None:
                    color = text_rgb
            d.rectangle([x, y, x+col_w[c], y+rh], fill=fill_color, outline=(230,230,230))
            if c == 0:
                for li, line in enumerate(wrapped_name[idx]):
                    d.text((x+10, y+10 + li*base_row_h), line, fill=(0,0,0), font=font_cell)
            else:
                tw = text_w(font_cell, t)
                d.text((x + col_w[c] - tw - 12, y+10), t, fill=color, font=font_cell)
            x += col_w[c]
        y += rh

    img.save(path, format="PNG")




# --- product short name (IMAGE ONLY) ---
# Only used for PNG rendering. DO NOT use this for DB keys / Excel exports.
DEFAULT_SHORTNAME_MAP = {
    "缙云": ["缙云"],
    "新力": ["新力"],
    "放8": ["放心8号", "放心8", "放8"],
    "放10": ["放心10号", "放心10", "放10"],
}

def short_product_name(full_name: str, max_len: int = 8) -> str:
    if not full_name:
        return ""
    t = str(full_name).strip()
    m = re.search(r"(SBGZ87|STR134|SXA927|SXZ218)", t, flags=re.I)
    if m:
        code = m.group(1).upper()
        mp = {
            "SBGZ87": "缙云",
            "STR134": "新力",
            "SXA927": "放10",
            "SXZ218": "放8",
        }
        return mp.get(code, code)
    n = t.replace("_", "")
    for short, keys in DEFAULT_SHORTNAME_MAP.items():
        for k in keys:
            if k in n:
                return short
    m = re.match(r'^([A-Z]{2,5}\d{2,6})', n)
    code = m.group(1) if m else ""
    tail = n[len(code):]
    tail = re.sub(r'大可|私募|证券|投资|基金|委托|资产|管理|有限公司', '', tail)
    tail = re.sub(r'\s+', '', tail)
    tail = tail[:max_len] if tail else n[:max_len]
    return (code + tail)[:max_len] if code else tail[:max_len]
