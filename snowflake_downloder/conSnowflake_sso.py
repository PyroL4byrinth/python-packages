##
# 20251128  first commit
# 20260217  add function:ConvertDatetimeFormat
# 20260309  branch:conSnowflake.py -> conSnowflake_sso.py
#           add main:Connect BDAP

# snowflake.connectorモジュールのインポート
import snowflake.connector              # Snowflake(Python Connector)でDBへ接続
import pandas as pd                     # データ分析・表形式データ操作
import toml                             # TOML設定ファイルの読み書き
import glob                             # ファイルパターンからファイル一覧取得
import os                               # OS機能（パス操作、環境変数、ファイル操作）
import sys                              # Python実行環境、引数、終了処理
import tempfile                         # 一時ファイル/ディレクトリ作成
import psutil                           # プロセス/メモリ/CPUなどのシステム情報取得
import logging                          # ログ出力
import json                             # Jsonファイルの読み書き
import shutil                           # 高水準のファイル操作
from datetime import  datetime          # 日時の取得・操作
from snowflake.snowpark import Session  # Snowflake Snowpark(Dataframe API)で処理
from pathlib import Path                # pathlibによるパス操作

# ログ設定
if not os.path.isdir('log'):
    os.makedirs('log') #[log]フォルダが無ければ生成

log_filename = datetime.now().strftime('log/%Y%m%d_%H%M%S.log')
log_file = open(log_filename, mode='a', encoding='utf-8')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

#TOMLファイルから接続情報を読み込む
try:
    # 本番用
    config = toml.load('old/sample_connections_sso.toml')
    
    sf = config['snowflake']
except FileNotFoundError:
    logger.error("'connections.toml' file not found")
    sys.exit(1)
except Exception as e:
    logger.error("Failed to lead the TOML file")
    logger.info(e)
    sys.exit(1)

# セッションパラメータの設定
# 接続(connection_parameters)：データベースに入るための「鍵」
try:
    connection_parameters = {
        "account": sf['account'],   
        "user": sf['user'],
        "role": sf['role'],
        "warehouse": sf['warehouse'],
        "schema": sf['scheme'],
        "authenticator": "externalbrowser",
    }
except Exception as e:
    logger.error("Failed to connect to Snowflake")
    logger.error(e)
    sys.exit(1)

## （認証専用）ブラウザを起動
# 外部ブラウザを起動したプロセスIDを残すファイルパスを生成
pid_file = Path(tempfile.gettempdir()) / "sf_extbrowser_pid.txt"  
pid_file.unlink(missing_ok=True) #前回分があれば削除
# ディレクトリ内にある browser_wrapper.py のファイルパスを絶対パスに解決
wrapper = (Path(__file__).with_name("browser_wrapper.py")).resolve()  
# 一時的に作ったブラウザを環境変数に設定
os.environ["BROWSER"] = f'"{sys.executable}" "{wrapper}" %s "{pid_file}"' 

## ブラウザ
def close_auth_browser():  
    # pid_fileが存在しなければ終了
    if not pid_file.exists():  
        return  
    # JSON（pid_file）からプロファイルを取得
    profile_dir = json.loads(pid_file.read_text(encoding="utf-8"))["profile_dir"]  
  
    targets = []  
    # 実行中プロセスを列挙
    for p in psutil.process_iter(["pid", "cmdline"]):  
        # コマンドラインからの返り値を連結
        cmd = " ".join(p.info.get("cmdline") or [])  
        #　コマンドライン内に対象プロファイルが含まれる場合
        if profile_dir in cmd:  
            # pidを取得
            pr = psutil.Process(p.info["pid"])  
            # 対象プロセスを停止対象へ追加
            targets.append(pr)  
            # そのプロセスが起動した子プロセスも再帰的に停止対象へ追加
            targets.extend(pr.children(recursive=True))  
    
    # PIDをキーにして重複削除
    uniq = {p.pid: p for p in targets}.values()  
    # プロセスの終了要求（terminate(穏当終了) or kill(強制終了))
    for p in uniq:  
        try: p.terminate()  
        except: pass  # 権限不足や終了済みのものは無視
    psutil.wait_procs(list(uniq), timeout=5)  
    for p in uniq:  
        try: p.kill()  
        except: pass  
  
    # profile_dirを削除
    shutil.rmtree(profile_dir, ignore_errors=True)  

# 認証
session = Session.builder.configs(connection_parameters).create()

# ブラウザを閉じる
close_auth_browser()

# SQLファイルを全て読み込み
if not os.path.isdir('sql'):
    logger.error("'sql' folder not found")
    session.close()
    sys.exit(1)
else:
    sql_all_files = glob.glob('sql/*.sql')

if not sql_all_files:
    logger.warning("'sql' There are no SQL files")
    session.close()
    sys.exit(1)

# SQLファイルの読み込み
for sql_file in sql_all_files:
    try:
        with open(sql_file, 'r', encoding='utf-8-sig') as file:
            sql_query = file.read()
        # SQL実行
        results = session.sql(sql_query)

        # pandasデータフレームに変換
        df = results.to_pandas()

        """
        # 型変換用関数
        def convDatetimeFormat(df, col_name):

            s = pd.to_datetime(df[col_name])
            s = s.dt.strftime('%Y-%m-%d %H:%M:%S.%f %z')
            return s.str.replace(r'(\.\d{3})\d{3}', r'\1', regex=True)

        #型変換
        cols = ['登録年月日', '更新年月日', 'データがフィックスされたタイミング']
        for col in cols:
            df[col]  = convDatetimeFormat(df, col)
        """        

        #　CSVファイルに保存(outputフォルダに保存)
        save_path = sql_file.split('\\')[-1].replace('.sql','.csv')
        csv_file = f"output/{save_path}"
        df.to_csv(csv_file, index=False, encoding='utf-8')

        logger.info("[SUCCESS] %s → %s",sql_file, csv_file)
    except Exception as e:
        logger.error("[FAILED] %s: %s", sql_file, e)

# 認証のクローズ
session.close()
# ログ出力
logger.info("全処理終了")