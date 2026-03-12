-- メーカマスタ
With x as(

    SELECT  
        t."メーカコード" as MakerCode,
        t."メーカ名称" as MakerName
    FROM  
        SELFANALYSIS_DB.GP_GPM_LOW.V_GPM_MAK_MST as t
),
p as (
    SELECT
        x.*,
        FLOOR((ROW_NUMBER() OVER (ORDER BY MakerCode) - 1) / 2000) + 1 as page_no
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