# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
entry = project_root / "client_app" / "app" / "main.py"

block_cipher = None

a = Analysis(
    [str(entry)],
    pathex=[str(project_root)],
    binaries=[],
    datas=[
        (str(project_root / "client_app" / "assets"), "assets"),
        (str(project_root / "client_app" / "config"), "config_src"),
    ],
    hiddenimports=[],
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
    name="DakeClient",
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
    name="DakeClient",
)
