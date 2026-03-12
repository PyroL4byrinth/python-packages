-- 品目カテゴリマスタ
With x as(

    SELECT  
        t."品目カテゴリ番号" as CategoryID,
        LEFT(
            t."品目カテゴリ番号",
            2
        ) as LargeID,
        t."品目大分類名称(日本語)" as LargeName,
        SUBSTR(
            t."品目カテゴリ番号",
            3,
            2
        ) as MediumID,
        t."品目中分類名称(日本語)" as MediumName,
        RIGHT(
            t."品目カテゴリ番号",
            2
        ) as SmallID,
        t."カテゴリ名称(日本語)" as SmallName     
    FROM  
        SELFANALYSIS_DB.GP_GPM_LOW.V_GPM_CATE_MST as t
),
p as (
    SELECT
        x.*,
        FLOOR((ROW_NUMBER() OVER (ORDER BY CategoryID) - 1) / 2000) + 1 as page_no
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