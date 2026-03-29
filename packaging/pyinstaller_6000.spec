# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
from PyInstaller.utils.hooks import collect_all

project_root = Path(globals().get("SPECPATH", ".")).resolve().parent
entry = project_root / "client_app" / "app" / "main_6000.py"

block_cipher = None

hiddenimports = [
    "uvicorn.logging",
    "uvicorn.config",
    "pyzmail",
    "pyzmail36",
    "requests",
    "dotenv",
    "fastapi",
    "starlette",
    "uvicorn",
    "numpy",
    "pandas",
    "openpyxl",
    "xlrd",
    "xlsxwriter",
    "multipart",
    "pydantic",
    "PySide6",
]

packages = [
    "pyzmail",
    "pyzmail36",
    "requests",
    "dotenv",
    "fastapi",
    "starlette",
    "uvicorn",
    "numpy",
    "pandas",
    "openpyxl",
    "xlrd",
    "xlsxwriter",
    "multipart",
    "pydantic",
    "PySide6",
]

datas, binaries, hidden = [], [], []
for pkg in packages:
    d, b, h = collect_all(pkg)
    datas += d
    binaries += b
    hidden += h

a = Analysis(
    [str(entry)],
    pathex=[str(project_root)],
    binaries=binaries,
    datas=datas + [
        (str(project_root / "client_app" / "app" / "services" / "xlsx_merge"), "client_app/app/services/xlsx_merge"),
        (str(project_root / "client_app" / "assets"), "client_app/assets"),
        (str(project_root / "client_app" / "config"), "client_app/config"),
    ],
    hiddenimports=hiddenimports + hidden,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="DakeClient6000",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    name="DakeClient6000",
)
