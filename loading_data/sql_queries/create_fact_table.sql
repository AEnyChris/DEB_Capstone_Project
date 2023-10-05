WITH CTE_amount_spent AS(
    SELECT
        customer_id,
        SUM(quantity * unit_price) AS amount_spent
    FROM
        sodium-mountain-396818.deb_capstone_dw.user_purchase
    GROUP BY
        customer_id
),
CTE_review AS (
    SELECT
        customer_id,
        SUM(is_positive) AS review_score,
        COUNT(review_id) AS review_count
    FROM
        sodium-mountain-396818.deb_capstone_dw.classified_movie_review
    GROUP BY
        customer_id
)

SELECT 
    cas.customer_id,
    id_dim_location,
    id_dim_devices,
    id_dim_os,
    amount_spent,
    review_score,
    review_count,
    insert_date
FROM CTE_amount_spent cas, dim_devices, dim_os, dim_location
JOIN CTE_review cr ON cas.customer_id = cr.customer_id
JOIN sodium-mountain-396818.deb_capstone_dw.classified_movie_review mr ON mr.customer_id = cas.customer_id


