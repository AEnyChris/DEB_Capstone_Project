WITH cte_distinct_os AS (
    SELECT DISTINCT os FROM sodium-mountain-396818.deb_capstone_dw.review_logs
)

SELECT
    ROW_NUMBER() OVER (ORDER BY os) AS id_dim_os,
    os
FROM 
    cte_distinct_os