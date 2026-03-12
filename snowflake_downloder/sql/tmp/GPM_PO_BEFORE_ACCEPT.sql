-- 発注済未検収
SELECT  
    * 
FROM  
    SELFANALYSIS_DB.GP_GPM_LOW.V_GPM_PO_BEFORE_ACCEPT
WHERE
    "購買依頼_明細_搬送先住所" = 'JAPAN 441-8074 愛知県 豊橋市 明海町3-23'
