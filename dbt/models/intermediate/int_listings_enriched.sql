{{ config(materialized='table') }}

WITH base AS (SELECT * FROM {{ ref('stg_raw_listings') }}),

phone_clean AS (
    SELECT *,
        REGEXP_REPLACE(COALESCE(phone, ''), '^0(\\d{10})$', '+44\\1') AS phone_e164
    FROM base
),

scored AS (
    SELECT *,
        (CASE WHEN phone IS NOT NULL THEN 20 ELSE 0 END +
         CASE WHEN website IS NOT NULL THEN 20 ELSE 0 END +
         CASE WHEN opening_hours::VARCHAR != '{}' THEN 20 ELSE 0 END +
         CASE WHEN categories::VARCHAR != '[]' THEN 20 ELSE 0 END +
         20) AS health_score
    FROM phone_clean
),

seo AS (
    SELECT *,
        CONCAT(name, ' — ', COALESCE(categories[0][0]::VARCHAR, 'local business'),
            ' in Glasgow, Scotland.',
            CASE WHEN rating IS NOT NULL THEN ' Rated ' || rating::VARCHAR || ' stars on Google.' ELSE '' END
        ) AS meta_description
    FROM scored
)

SELECT * FROM seo WHERE health_score >= 40
