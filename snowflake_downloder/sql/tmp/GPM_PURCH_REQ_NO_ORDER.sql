-- 購入依頼済未発注
SELECT  
    * 
FROM  
    SELFANALYSIS_DB.GP_GPM_LOW.V_GPM_PURCH_REQ_NO_ORDER
WHERE
    "購買依頼_明細_搬送先住所" = 'JAPAN 441-8074 愛知県 豊橋市 明海町3-23'