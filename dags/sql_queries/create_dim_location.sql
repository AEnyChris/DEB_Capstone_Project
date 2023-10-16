WITH cte_distinct_location AS (
    SELECT DISTINCT location FROM review_logs
)

SELECT
    ROW_NUMBER() OVER (ORDER BY location) AS id_dim_location,
    location
FROM 
    cte_distinct_location;