CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE staging.raw_listings (
    place_id        VARCHAR(255) PRIMARY KEY,
    name            TEXT NOT NULL,
    address         TEXT,
    phone           VARCHAR(50),
    website         TEXT,
    postcode        VARCHAR(10),
    latitude        DECIMAL(10,7),
    longitude       DECIMAL(10,7),
    categories      JSONB DEFAULT '[]',
    opening_hours   JSONB DEFAULT '{}',
    rating          DECIMAL(2,1),
    review_count    INTEGER,
    photo_urls      JSONB DEFAULT '[]',
    business_status VARCHAR(50) DEFAULT 'OPERATIONAL',
    scraped_at      TIMESTAMPTZ DEFAULT NOW(),
    content_hash    VARCHAR(64),
    pipeline_status VARCHAR(30) DEFAULT 'NEW'
        CHECK (pipeline_status IN
          ('NEW','STAGED','TRANSFORMED','PUBLISHED','CLOSED','FAILED')),
    wp_post_id      INTEGER,
    last_synced_at  TIMESTAMPTZ
);

CREATE INDEX idx_status  ON staging.raw_listings(pipeline_status);
CREATE INDEX idx_scraped ON staging.raw_listings(scraped_at);

CREATE VIEW staging.pending_updates AS
SELECT s.* FROM staging.raw_listings s
WHERE pipeline_status IN ('NEW','STAGED')
   OR (pipeline_status = 'PUBLISHED'
       AND content_hash IS DISTINCT FROM
           (SELECT content_hash FROM staging.raw_listings r
            WHERE r.place_id = s.place_id AND r.wp_post_id IS NOT NULL));
```

Save and exit. Then apply it to your Postgres container:
```
docker compose exec postgres psql -U pipeline -d glasgow_traders -f /dev/stdin < staging/schema.sql
