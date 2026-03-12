-- 仕入先マスタ
With x as(

    SELECT  
        t."サプライヤサイトコード" as SupplierCode,
        t."サプライヤ番号" as SupplierNo,
        t."サプライヤ名" as SupplierName,
        t."仕入先略称名(漢字)" as ShortName,
        t."サプライヤサイト名" as SiteName
    FROM  
        SELFANALYSIS_DB.GP_GPM_LOW.V_GPM_DATA_PROV_SUPPLIER_MASTER as t
),
p as (
    SELECT
        x.*,
        FLOOR((ROW_NUMBER() OVER (ORDER BY SupplierCode) - 1) / 2000) + 1 as page_no
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
