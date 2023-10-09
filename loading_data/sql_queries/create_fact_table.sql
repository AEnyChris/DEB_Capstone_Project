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
    cmr.customer_id,
    id_dim_devices,
    id_dim_location,
    id_dim_os,
    amount_spent,
    review_score,
    review_count,
    insert_date
FROM sodium-mountain-396818.deb_capstone_dw.review_logs rl
JOIN sodium-mountain-396818.deb_capstone_dw.dim_devices ddv ON ddv.device=rl.device
JOIN sodium-mountain-396818.deb_capstone_dw.dim_location dloc ON dloc.location=rl.location
JOIN sodium-mountain-396818.deb_capstone_dw.dim_os dos ON dos.os=rl.os
JOIN sodium-mountain-396818.deb_capstone_dw.classified_movie_review cmr ON cmr.review_id=rl.log_id
JOIN CTE_review ON CTE_review.customer_id=cmr.customer_id
JOIN CTE_amount_spent ON CTE_amount_spent.customer_id = cmr.customer_id

