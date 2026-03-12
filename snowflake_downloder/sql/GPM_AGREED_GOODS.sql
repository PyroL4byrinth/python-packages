-- 品目マスタ
With x as(

    SELECT  
        t."品目コード" as AgreedCode, 
        t."品目カテゴリ" as Category, 
        t."サプライヤサイトコード" as SupplierCode, 
        t."サプライヤ名" as SupplierName, 
        t."グローバル企業コード" as MakerCode, 
        t."単位コード" as UnitCode, 
        t."メーカ型式" as TypeName, 
        t."商品名" as ProductName, 
        t."単価" as UnitPrice
    FROM  
        SELFANALYSIS_DB.GP_GPM_LOW.V_GPM_AGREED_GOODS as t
),
p as (
    SELECT
        x.*,
        FLOOR((ROW_NUMBER() OVER (ORDER BY AgreedCode) - 1) / 2000) + 1 as page_no
    FROM
        x
)
SELECT
    p.* EXCLUDE(page_no),
    CASE
        WHEN page_no <= 26 THEN
            CHR(64 + page_no)
        ELSE
            CHR(64 + FLOOR((page_no - 1) / 26))
            ||
            CHR(64 + FLOOR((page_no - 1) / 26))
    END as pages
FROM
    p;