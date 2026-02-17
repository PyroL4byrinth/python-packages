## ワークスペース初期化
rm(list = ls()) # 現環境のオブジェクトをすべて解放（削除）する
# gc():garbage collection:未使用のメモリを回収
gc(); gc()

## パッケージ読み込み & インストール
Force <- FALSE
pkgs <- c("dplyr", "data.table", "ggplot2", "plotly","caret")
for (pkg in pkgs){
  # 読み込みに失敗したらインストールしてから再読み込み
  # require()：パッケージの読み込み時、失敗した場合FLASEを返して続行する関数
  # library()：パッケージの読み込み時、失敗した場合エラーで停止する関数
  # character.only = TURE:パッケージ名を文字列で渡す（デフォルトは、シンボル）
  if(Force || !require(pkg, character.only = TRUE)){
    install.packages(pkg)
    library(pkg, character.only = TRUE)
  }
}

## 自作関数の読み出し
funcs <- c("fit_model.R", "predict_model.R")
for (func in funcs){
  source(func)
}

## データソース変数
DATA_SOURCES <- "C:/GitHub/data-stored/lin2.csv"
PROCESS_NUMBER <- "./source/ProcessNumber.csv"
X_NAME <- "sum_equip_item_name - ピンSA溶接_測定値|溶接電圧No.4 - value"
Y_NAME <- "sum_equip_item_name - 部品組付_測定値|Oﾘﾝｸﾞ塗布流量 - value"

## CSV読み込み
df <- read.csv(DATA_SOURCES, header = TRUE, check.names = FALSE)
df_xy <- df[c(X_NAME, Y_NAME)]

##学習・検証（X複数、Y複数もOK） 
n = nrow(df_xy)
idx <- sample.int(n, size = floor(0.9 * n))

df_train <- df_xy[idx, ] #全体の9割を学習データ
df_test <- df_xy[-idx, ] #全体の3割を検証データ
  
m <- fit_condfreq(
  df_train[[X_NAME]],
  df_train[[Y_NAME]]
)

# 予測
res_list <- lapply(na.omit(df_test[[X_NAME]]), function(x) {
  list(
    x = x,
    pred = predict_condfreq(m, as.double(x), type = "class")
  )
})

df_out <- do.call(rbind, lapply(res_list, function(z) {  
  data.frame(  
    x    = z$x,  
    pred = as.character(z$pred),  
    stringsAsFactors = FALSE  
  )  
}))  

write.csv(df_out, "./output/res_list.csv", row.names = FALSE, fileEncoding = "UTF-8")  
# 予測
res <- predict_condfreq(m, 13.92)

# グラフ化
p <- ggplot(res, aes(x = res$y, y = res$p)) +
      geom_point() +
      geom_line()

ggplotly(p)
