WITH cte_distinct_log_date AS (
    SELECT DISTINCT log_date FROM review_logs
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY log_date) AS id_dim_date,
    log_date,
    SUBSTR(log_date,7,4) AS year,
    SUBSTR(log_date,1,2 ) AS month,
    SUBSTR(log_date,4,2) AS day
FROM 
    cte_distinct_log_date