# 一度だけ実行（もしくは手で state.json を削除）
import os
if os.path.exists("state.json"):
    os.remove("state.json")
print("state reset.")