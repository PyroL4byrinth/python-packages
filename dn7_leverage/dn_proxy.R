
# DN社内のネットワーク接続時に実行
# 自宅なでVPN仕様せずRを動かす場合は実行不要

dns_access <- TRUE
if(dns_access){
  Sys.setenv("http_proxy" = "http://in-proxy.denso.co.jp:8080")
  Sys.setenv("https_proxy" = "http://in-proxy.denso.co.jp:8080")
}else{
  Sys.setenv("http_proxy" = "http://133.192.24.101:8080")
  Sys.setenv("https_proxy" = "http://133.192.24.101:8080")
}
