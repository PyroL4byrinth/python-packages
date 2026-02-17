#############################################################
#
# 値型判別用プログラム
# データセットの10000行を対象に下記ルールに基づき型判定を行う
# [ルール]
#   [出力する（予測に使う）]
#   ・低濃度          ：ユニーク数が100以下
#   ・中濃度（整数）  ：低濃度・高濃度に含まれない整数値
#   ・中濃度（実数）  ：低濃度・高濃度に含まれない実数値
#   ・高濃度（数値系）：ユニーク数が20％以上かつ、単調性がない
#   [出力しない（予測に使わない）]
#   ・高濃度（ID系）  ：ユニーク数が20％以上かつ、単調性をある
#   ・カテゴリ        ：数値以外（文字型）
#   ・二値            ：0/1(true/false, on/off...)
#   ・低数値          ：ユニーク数が1
#
#############################################################

## ワークスペース初期化
rm(list = ls()) # 現環境のオブジェクトをすべて解放（削除）する
# gc():garbage collection:未使用のメモリを回収
gc(); gc()

## パッケージ読み込み & インストール
Force <- FALSE
pkgs <- c("dplyr", "data.table")
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

## データソース変数
DATA_SOURCES <- "C:/GitHub/data-stored/lin2.csv"
PROCESS_NUMBER <- "./source/ProcessNumber.csv"

## CSV読み込み
df <- read.csv(DATA_SOURCES, header = TRUE, check.names = FALSE)
df <- df[1:100000,] # 100000行で判定

#ヘッダー加工
col_names <- names(df) %>%
  gsub(" - ", "_", ., fixed = TRUE) %>%
  gsub("||", "_", ., fixed = TRUE) %>%
  gsub("|", "_", ., fixed = TRUE) %>%
  strsplit("_",fixed = TRUE)

lens <- lengths(col_names)

# lapply(X, FUN) : Xの要素を1つずつ取り出してFUNを適用してlistで返す関数
nm <- lapply(col_names, function(x){ 
  if(length(x) == 7){
    x <- append(x, "測定値", after = 5)
  }else if(length(x) < 8){
    # 要素数が8になるようにxにNA値を入れる
    # rep(x, n) :同じ値(x)を繰り返し(n)作る関数
    # NA_character_ : 文字列の欠損値
    x <- c(x, rep(NA_character_, 8 - length(x)))
  }else if(length(x) > 8){
    # 要素数が8になるようにxを制限
    x <- c(x[1:8])
  }
  x # 返り値
})

# stopifnot() : 渡した条件がすべてTUREなら通過、それ以外の場合はエラーで止める関数
stopifnot(all(lengths(nm) == 8))
# do.call(FUN, X) : Xを引数列として展開してFUNを1回だけ適用する関数
df_category <- as.data.frame(do.call(rbind, nm), stringsAsFactors = FALSE)
colnames(df_category) <- c("v1","v2","v3","v4","machine","item","category","v8")

## 値型を列結合
# 値型判定用関数
classify_col <- function(x, nm){
  
  #単調増加・減少・その他判定用関数
  mono_type <- function(x){
    # diff(x) : x[i+1] - x[i]の差分のベクトルを返す関数
    d <- diff(x) 
    if (all(d >= 0)) return("non-decreasing")
    if (all(d <= 0)) return("non-increasing")
    "not-monotone" # 返り値
  }
  
  x_chr <- as.character(x) 
  x_num <- suppressWarnings(as.numeric(x_chr))
  has_string <- any(!is.na(x_chr) & x_chr != "" & is.na(x_num))
  if (has_string) return("カテゴリ値")
  
  if(grepl("OK/NG情報", nm, fixed = TRUE)) return("二値")
  
  n <- length(unique(na.omit(x_num)))
  if(n == 1) return("定数値")
  if(n < 100) return("低濃度値")
  if(n/10000 > 0.2) {
    if(mono_type(na.omit(x_num)) == "not-monotone"){
      return("高濃度(数値系）")
    }else{
      return("高濃度（ID系）") 
    }
  }
  is_real <- any(abs(x_num - round(x_num)) > 1e-8, na.rm = TRUE)
  if(is_real) "実数値" else "整数値"
}

## 値型を判定
types <- sapply(names(df), function(nm) classify_col(df[[nm]], nm))
df_type <- data.frame(col = names(types), type = unname(types), row.names = NULL)

## フルネームと値型を列結合
df_full_col <- dplyr::bind_cols(df_category, fullname = names(df)) %>%
  dplyr::bind_cols(type = df_type$type)

## 行程番号を列結合
df_cross <- read.csv(PROCESS_NUMBER, header = TRUE, check.names = FALSE)
df_label <- df_cross %>%
  right_join(df_full_col %>% select("machine", "item", "category", "fullname", "type"),by = "machine")

#必要なものだけ抽出
          
df_label <- df_label[!(df_label$type %in% c("カテゴリ値","定数値","高濃度(ID)")),]

#出力
write.csv(df_label, "./output/category_lin2.csv", row.names = FALSE, fileEncoding = "shift-jis")

