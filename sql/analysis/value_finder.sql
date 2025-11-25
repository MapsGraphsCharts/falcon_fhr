WITH params AS (
    SELECT
        0.9   AS threshold,
        3000.0 AS median_cap -- exclude ultra-high rates when computing median
),
     nightly AS (
    SELECT
        rs.id AS rate_snapshot_id,
        h.name AS hotel_name,
        h.address_city AS hotel_city,
        h.address_state AS hotel_state,
        h.address_country_name AS hotel_country,
        rs.property_id,
        rt.name AS room_type_name,
        rs.room_type_id,
        sr.destination_name,
        DATE(sr.check_in) AS check_in_date,
        strftime('%Y-%m', sr.check_in) AS check_in_month,
        sr.nights,
        rs.pricing_currency,
        COALESCE(
                rs.average_nightly_rate,
                CASE WHEN sr.nights > 0 THEN rs.pricing_total_inclusive / sr.nights END
        ) AS nightly_rate,
        rs.hotel_collection,
        rs.available
    FROM rate_snapshots rs
             JOIN search_runs sr ON sr.id = rs.run_id
             JOIN hotels h ON h.property_id = rs.property_id
             LEFT JOIN room_types rt
                       ON rt.property_id = rs.property_id
                           AND rt.room_type_id = rs.room_type_id
    WHERE rs.pricing_total_inclusive IS NOT NULL
      AND sr.nights > 0
      AND rs.average_nightly_rate IS NOT NULL
),
     ranked AS (
         SELECT
             n.*,
             ROW_NUMBER() OVER (
                 PARTITION BY property_id, room_type_id, check_in_month
                 ORDER BY nightly_rate
                 ) AS rn,
             COUNT(*) OVER (
                 PARTITION BY property_id, room_type_id, check_in_month
                 ) AS cnt,
             AVG(nightly_rate) OVER (
                 PARTITION BY property_id, room_type_id, check_in_month
                 ) AS monthly_avg
         FROM nightly n
     ),
     monthly_medians AS (
         SELECT
             property_id,
             room_type_id,
             check_in_month,
             AVG(nightly_rate) AS monthly_median
         FROM ranked
         WHERE rn IN ((cnt + 1) / 2, (cnt + 2) / 2)
         GROUP BY property_id, room_type_id, check_in_month
     ),
     ranked_capped AS (
         SELECT
             n.*,
             ROW_NUMBER() OVER (
                 PARTITION BY property_id, room_type_id, check_in_month
                 ORDER BY nightly_rate
                 ) AS rn,
             COUNT(*) OVER (
                 PARTITION BY property_id, room_type_id, check_in_month
                 ) AS cnt
         FROM nightly n, params p
         WHERE n.nightly_rate <= p.median_cap
     ),
     monthly_medians_capped AS (
         SELECT
             property_id,
             room_type_id,
             check_in_month,
             AVG(nightly_rate) AS monthly_median
         FROM ranked_capped
         WHERE rn IN ((cnt + 1) / 2, (cnt + 2) / 2)
         GROUP BY property_id, room_type_id, check_in_month
     )
SELECT
    r.destination_name,
    r.hotel_collection,
    r.hotel_name,
    r.hotel_city,
    r.hotel_state,
    r.hotel_country,
    r.room_type_name,
    r.check_in_date,
    r.check_in_month,
    r.nightly_rate,
    r.monthly_avg,
    COALESCE(mc.monthly_median, m.monthly_median) AS monthly_median,
    ROUND(r.nightly_rate / NULLIF(r.monthly_avg, 0), 3) AS value_vs_avg,
    ROUND(r.nightly_rate / NULLIF(COALESCE(mc.monthly_median, m.monthly_median), 0), 3) AS value_vs_median
FROM ranked r
         JOIN monthly_medians m
              ON m.property_id = r.property_id
                  AND m.room_type_id = r.room_type_id
                  AND m.check_in_month = r.check_in_month
         LEFT JOIN monthly_medians_capped mc
                   ON mc.property_id = r.property_id
                       AND mc.room_type_id = r.room_type_id
                       AND mc.check_in_month = r.check_in_month
CROSS JOIN params p
WHERE r.nightly_rate <= p.threshold * COALESCE(mc.monthly_median, m.monthly_median)
AND r.check_in_month = '2026-01'
AND r.hotel_country = 'Japan'
ORDER BY r.nightly_rate / NULLIF(COALESCE(mc.monthly_median, m.monthly_median), 0) ASC,
         r.nightly_rate ASC
LIMIT 1000;
