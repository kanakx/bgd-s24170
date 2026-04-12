SELECT
    l.neighbourhood,
    l.neighbourhood_group,
    DATE_TRUNC('month', c.calendar_date)::DATE                          AS month,
    COUNT(*)                                                            AS total_days,
    SUM(CASE WHEN c.available THEN 1 ELSE 0 END)                       AS available_days,
    ROUND(
        100.0 * SUM(CASE WHEN c.available THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                                                   AS availability_pct,
    ROUND(AVG(c.price), 2)                                              AS avg_daily_price
FROM {{ ref('stg_calendar') }} c
JOIN {{ ref('stg_listings') }} l ON c.listing_id = l.listing_id
GROUP BY l.neighbourhood, l.neighbourhood_group, DATE_TRUNC('month', c.calendar_date)
ORDER BY month, l.neighbourhood_group, l.neighbourhood
