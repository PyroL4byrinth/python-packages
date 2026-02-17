## 依存関係の明確化
if (!requireNamespace("data.table", quietly = TRUE)){
  stop("Package 'data.table' is required.")
}

###条件付き頻度(Conditional Frequency)
fit_condfreq <- function(x_data, y_data, alpha = 1, na_level = "<NA>"){
  
  ## 入力をdata.frame化（ベクトルでもOK）
  if (is.vector(x_data) && !is.list(x_data)) x_data <- data.frame(x_data)
  if (is.vector(y_data) && !is.list(y_data)) y_data <- data.frame(y_data)
  
  X <- as.data.table(x_data)
  Y <- as.data.table(y_data)
  
  ## 仮の名前を付ける
  # paste0() : 区切り文字なしで結合する関数
  # seq_len(n) : 要素数n個のベクトルに対して、1, 2, …, nという連番を作る関数
  setnames(X, paste0("X", seq_len(ncol(X)))) #列名を付与：X1, X2,…
  setnames(Y, paste0("Y", seq_len(ncol(Y)))) #          ：y1, y2,…
  
  x_cols <- names(X)
  y_cols <- names(Y)
  
  DT <- cbind(X, Y)
  DT <- na.omit(DT)
  
  y_models <- lapply(y_cols, function(y_col){
    
    y_levels <- sort(unique(DT[[y_col]]))
    K <- length(y_levels)
    
    ## x,yの頻度表の作成
    # .N : 「そのグループの行数」を表す特別な変数(Nという列が生成される)
    tab <- DT[, .N, by = c(x_cols, y_col)]
    setnames(tab, y_col, "y")
  
    ## xの頻度表の作成
    nx <- DT[, .N, by = c(x_cols)]
    setnames(nx, "N", "Nx")
    
    ## yの頻度表の作成
    marg <- DT[, .N, by = y_col]
    setnames(marg, y_col, "y")
    
    ## 周辺分布(marginal)処理
    # 周辺分布 : 未観測xの時、P(Y|X)の代わりに、P(Y)を返す
    # J        : Jの引数をキーにして結合し、存在しない行はキー以外の列がNAで追加される
    # on = "y" : 結合キーの指定
    marg <- marg[J(y_levels), on = "y"]
    ##NAの値を0にする
    # := : data.table専用の「参照代入」演算子。data.tableのコピーを作らず、その場で更新する
    # 0L : 整数の0
    marg[is.na(N), N:= 0L]
    ## 確率pに変換（加法スムージング）
    # alpha : 加法ラプラススムージング（確率が0になるのを防ぐ）
    marg[, p := (N + alpha) / (sum(N) + alpha * K)]
    ## 必要な列のみ残す(N列を落とす)
    marg <- marg[, list(y, p)]
    
    ##キー設定（後続処理（結合）を高速に行うため）
    # setkeyv(data.table, key) :data.tableにkeyを設定して、keyでソートを行う
    setkeyv(tab, x_cols)
    setkeyv(nx, x_cols)
    
    ## 各頻度表をリストにする
    list(y_col=y_col, y_levels=y_levels, K=K, tab=tab, nx=nx, marg=marg, alpha=alpha) # 返り値
    
  })  

  ## モデルに目的変数名を付ける
  names(y_models) <- y_cols
  
  ## オブジェクト化
  # structure() : オブジェクトに属性を付与して返す関数
  structure(list(x_cols=x_cols, y_models=y_models, na_level=na_level), class = "coondfreq_model") # 返り値
  
}