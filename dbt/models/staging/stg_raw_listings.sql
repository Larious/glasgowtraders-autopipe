{{ config(materialized='view') }}

SELECT
    place_id, name, address, phone, website,
    postcode, latitude, longitude, categories,
    opening_hours, rating, review_count,
    business_status, pipeline_status,
    content_hash, wp_post_id, scraped_at
FROM staging.raw_listings
WHERE pipeline_status IN ('STAGED', 'TRANSFORMED')
