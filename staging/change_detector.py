import hashlib
import json

def generate_hash(data: dict) -> str:
    hash_string = f"{data.get('name')}|{data.get('address')}|{data.get('phone')}|{data.get('website')}|{data.get('hours')}"
    return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()

def upsert_to_staging(conn, data: dict):
    data_hash = generate_hash(data)
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO staging.raw_listings (
                place_id, name, address, phone, website, postcode, 
                latitude, longitude, categories, opening_hours, rating, review_count, business_status, data_hash
            ) VALUES (
                %(place_id)s, %(name)s, %(address)s, %(phone)s, %(website)s, %(postcode)s,
                %(lat)s, %(lng)s, %(categories)s, %(hours)s, %(rating)s, %(review_count)s, %(status)s, %(data_hash)s
            )
            ON CONFLICT (place_id) DO UPDATE SET
                name = EXCLUDED.name,
                address = EXCLUDED.address,
                phone = EXCLUDED.phone,
                website = EXCLUDED.website,
                postcode = EXCLUDED.postcode,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                categories = EXCLUDED.categories,
                opening_hours = EXCLUDED.opening_hours,
                rating = EXCLUDED.rating,
                review_count = EXCLUDED.review_count,
                business_status = EXCLUDED.business_status,
                data_hash = EXCLUDED.data_hash,
                updated_at = CURRENT_TIMESTAMP
            WHERE staging.raw_listings.data_hash != EXCLUDED.data_hash;
        ''', {**data, "data_hash": data_hash})
    conn.commit()
