import hashlib
import json

def compute_hash(business: dict) -> str:
    """SHA-256 of key fields — if this changes, re-publish"""
    key_fields = {
        "name":   business.get("name"),
        "phone":  business.get("phone"),
        "address": business.get("address"),
        "hours":  json.dumps(business.get("opening_hours", {}), sort_keys=True),
        "status": business.get("business_status"),
        "rating": business.get("rating"),
    }
    payload = json.dumps(key_fields, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()

def upsert_to_staging(conn, business: dict):
    new_hash = compute_hash(business)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO staging.raw_listings
              (place_id, name, address, phone, website, postcode,
               latitude, longitude, categories, opening_hours,
               rating, review_count, business_status, content_hash)
            VALUES
              (%(place_id)s, %(name)s, %(address)s, %(phone)s,
               %(website)s, %(postcode)s, %(lat)s, %(lng)s,
               %(categories)s::jsonb, %(hours)s::jsonb,
               %(rating)s, %(review_count)s, %(status)s, %(hash)s)
            ON CONFLICT (place_id) DO UPDATE SET
              content_hash    = EXCLUDED.content_hash,
              pipeline_status = CASE
                WHEN staging.raw_listings.content_hash != EXCLUDED.content_hash
                THEN 'STAGED' ELSE staging.raw_listings.pipeline_status END,
              last_synced_at  = NOW()
        """, {**business, "hash": new_hash})
    conn.commit()
```

Save and exit. Then commit:
```
git add -A
git commit -m "phase-03: staging schema and SHA-256 change detector"
git push
