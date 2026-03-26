Dake Client (Windows)

包含两个独立程序：
1. DakeClient6002.exe
   用途：6002 估值自动化，本地抓取/生成/推送。
   数据目录：.\\data\\6002
   配置文件：.\\config\\config.json

2. DakeClient6000.exe
   用途：6000 合并工具。
   数据目录：.\\data\\6000
   配置文件：.\\config\\config.json

首次使用：
- 先编辑 .\\config\\config.json
- 再启动对应 exe

说明：
- 两个程序互相独立
- 6002 默认会使用本机 127.0.0.1:16002 / 16000 作为本地服务端口
