## 依存関係の明確化
if (!requireNamespace("data.table", quietly = TRUE)){
  stop("Package 'data.table' is required.")
}

###条件付き頻度(Conditional Frequency)
predict_condfreq <- function(model, x, y_col = NULL, type = c("class", "prob")){
  
  type <- match.arg(type)
  
  ## どのYを返すか（省略時は学習したY全部）
  target_y <- if(is.null(y_col)) names(model$y_models) else as.character(y_col)
  
  ## xを整形
  xdt <- as.data.frame(x)
  if (nrow(xdt) != 1) stop("xは１点で渡してください")
  #xdt <- as.character((xdt))
  xdt[is.na(xdt)] <- model$na_level
  
  ## 仮の名前を付ける
  # paste0() : 区切り文字なしで結合する関数
  # seq_len(n) : 要素数n個のベクトルに対して、1, 2, …, nという連番を作る関数
  setnames(xdt, paste0("X", seq_len(ncol(xdt)))) #列名を付与：X1, X2,…
  
  ## yを予測
  predict_y <- function(m) {
    
    ## Xの出現回数Nxを取得（なければ未観測X）
    # DT[i, j , on=col] : iに一致する行をData.table(DT)から探し、その時のcolを参照する
    Nx = m$nx[xdt, x.Nx, on = model$x_cols]
    if (length(Nx) == 0 || is.na(Nx)){
      # 周辺分布を返す（copyは安全のため）
      return(copy(m$marg)) 
    }
    
    ## Xにおけるy別のカウントN(x, y)を取得
    # nomatch : 一致行がないとき（0L : NA行を作らず0行にする）
    # .() : list()と同義（省略形）、列をdata.tableとして返す場合はlistで返す
    # list(new_name = oldname) : old_nameをnew_nameに変えて、data.tableで返す
    m$tab[xdt, on = model$x_cols, nomatch = 0L][, .(y, n_xy = N)]
    cnt <- m$tab[xdt, on = model$x_cols, nomatch = 0L][, .(y, n_xy = N)]
    
    ## 全yレベルにそろえて、出現しないyは0件として確率化
    # merge(DT1, DT2, by = key, all.x = boolean) : DT1とDT2をkey列で左外部結合(all.x = TRUE) or 内部結合(all.x = FALSE)する
    out <- data.table(y = m$y_levels)
    out <- merge(out, cnt, by = "y", all.x = TRUE)
    out[is.na(n_xy), n_xy := 0L]
    
    ## 条件付き確率を計算してp列に追加・更新
    out[, p := (n_xy + m$alpha) / (Nx + m$alpha * m$K)]
    out[, .(y, p)]
    
  }

  ## 返り値生成
  if (length(target_y) == 1){
    #data.tableで返す
    res <- predict_y(model$y_models[[target_y]])
  }else{
    #listで返す
    res <- lapply(target_y, function(nm) predic_y(model$ymodels[[nm]]))
    names(res) <- target_y
  }
  if (type == "prob"){
    # 確率密度曲線を返り値とする
    return(res)
  }else{
    #最大確率の値を返り値とする
    return(res[which.max(p), y])
  }
}