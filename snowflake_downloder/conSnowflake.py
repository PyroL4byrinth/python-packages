##
# 20251128 first commit
# 20260217 add function:ConvertDatetimeFormat

# snowflake.connectorモジュールのインポート
import snowflake.connector
from cryptography.hazmat.primitives import serialization
import pandas as pd
import toml
import glob
import os
import sys
import logging 
from datetime import  datetime

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
    #　テスト用
    # config = toml.load('./old/connections.toml')
    # 本番用
    config = toml.load('connections.toml')
    
    sf = config['snowflake']
except FileNotFoundError:
    logger.error("'connections.toml' file not found")
    sys.exit(1)
except Exception as e:
    logger.error("Failed to lead the TOML file")
    sys.exit(1)

# セッションパラメータの設定
# 接続(conn)：データベースに入るための「鍵」
try:
    conn = snowflake.connector.connect(
        user=sf['user'],
        #password=sf['password'],
        account=sf['account'],
        role=sf['role'],
        warehouse=sf['warehouse'],
        # オプション
        scheme=sf['scheme'] if 'scheme' in sf else None,
        database=sf['database'] if 'database' in sf else None,
        private_key_file= sf['private_key_file'] if 'private_key_file' in sf else None,
        private_key_file_pwd=sf['private_key_file_pwd'] if 'private_key_file_pwd' in sf else None
    )
except Exception as e:
    logger.error("Failed to connect to Snowflake")
    sys.exit(1)

# カーソルの作成
# cur：データベースに対する操作を行うための「手」
cur = conn.cursor()

# SQLファイルを全て読み込み
if not os.path.isdir('sql'):
    logger.error("'sql' folder not found")
    cur.close()
    conn.close
    sys.exit(1)
else:
    sql_all_files = glob.glob('sql/*.sql')

if not sql_all_files:
    logger.warning("'sql' There are no SQL files")
    cur.close()
    conn.close()
    sys.exit(1)

# SQLファイルの読み込み
for sql_file in sql_all_files:
    try:
        with open(sql_file, 'r', encoding='utf-8-sig') as file:
            sql_query = file.read()
        # SQL実行
        cur.execute(sql_query)
        results = cur.fetchall()

        # データフレームに変換
        df = pd.DataFrame(results, columns=[col[0] for col in cur.description])

        # 型変換用関数
        def convDatetimeFormat(df, col_name):

            s = pd.to_datetime(df[col_name])
            s = s.dt.strftime('%Y-%m-%d %H:%M:%S.%f %z')
            return s.str.replace(r'(\.\d{3})\d{3}', r'\1', regex=True)

        #型変換
        cols = ['登録年月日', '更新年月日', 'データがフィックスされたタイミング']
        for col in cols:
            df[col]  = convDatetimeFormat(df, col)
        

        #　CSVファイルに保存(outputフォルダに保存)
        save_path = sql_file.split('\\')[-1].replace('.sql','.csv')
        csv_file = f"output/{save_path}"
        df.to_csv(csv_file, index=False, encoding='utf-8')

        logger.info("[SUCCESS] %s → %s",sql_file, csv_file)
    except Exception as e:
        logger.error("[FAILED] %s: %s", sql_file, e)


# カーソルと接続のクローズ
cur.close()
conn.close()
logger.info("全処理終了")
