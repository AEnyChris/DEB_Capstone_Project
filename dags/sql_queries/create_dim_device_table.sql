WITH cte_distinct_devices AS (
    SELECT DISTINCT device FROM sodium-mountain-396818.deb_capstone_dw.review_logs
)


SELECT
    ROW_NUMBER() OVER (ORDER BY device) AS id_dim_devices,
    device
FROM 
    cte_distinct_devices