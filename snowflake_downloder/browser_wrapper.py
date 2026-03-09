import sys                  # sys.argvコマンドライン引数を受け取る
import json                 # Pythonのdict型とJSON文字列の変換
import subprocess           # 外部プログラム(edge.exe)起動
import tempfile             # OSの一時ディレクトリパスを取得
import uuid                 # プロファイルフォルダ名に使うUID作成
from pathlib import Path    # パス操作

## 親プロセスから渡される引数取得
url = sys.argv[1]               # 開くべきURL
state_file = Path(sys.argv[2])  # 起動情報を親に返すためのファイルパス
  
## Edge本体の実行パス取得
EDGE = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"  

## 専用のプロファイルフォルダを作る
# OSの一時ディレクトリに乱数で作ったuidフォルダパスを生成
profile_dir = Path(tempfile.gettempdir()) / f"sf_profile_{uuid.uuid4().hex}"  
profile_dir.mkdir(parents=True, exist_ok=True)  # フォルダ作成

## Edgeを別のインスタンスとして起動
p = subprocess.Popen([  
    EDGE,                                   # 実行ファイル
    f"--app={url}",                         # 単独のアプリ風ウィンドウで{url}を開く
    f"--user-data-dir={profile_dir}",       # プロファイルとして使うフォルダを指定
    "--no-first-run",                       # 初回起動ダイアログを抑制
    "--no-default-browser-check",           # 同上
])  
  
## プロファイルとして使ったフォルダをJsonに書き残す
state_file.write_text(json.dumps({"profile_dir": str(profile_dir)}), encoding="utf-8")  