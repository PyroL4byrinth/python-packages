
import toml
import snowflake.connector
import pandas as pd
import os

def get_dataframe(sql: str, config_path="config.toml") -> pd.DataFrame:
    # プロキシ設定（必要なら）
    os.environ['HTTPS_PROXY'] = 'http://in-proxy-o.denso.co.jp:8080'
    os.environ['HTTP_PROXY'] = 'http://in-proxy-o.denso.co.jp:8080'

    # 設定読み込み
    config = toml.load(config_path)["snowflake"]

    # 接続
    conn = snowflake.connector.connect(
        user=config["user"],
        password=config["password"],
        account=config["account"],
        role=config["role"],
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"]
    )

    # SQL実行
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()

    return pd.DataFrame(rows, columns=columns)
