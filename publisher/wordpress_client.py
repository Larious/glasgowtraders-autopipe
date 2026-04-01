import requests
from requests.auth import HTTPBasicAuth

class WordPressPublisher:
    def __init__(self, base_url, username, app_password):
        self.base = base_url.rstrip('/')
        self.auth = HTTPBasicAuth(username, app_password)
        self.session = requests.Session()
        self.session.auth = self.auth

    def publish(self, listing):
        existing_id = self._find_by_place_id(listing['place_id'])

        payload = {
            "title": listing['name'],
            "status": "publish",
            "post_category": [315],
            "street": listing.get('address', ''),
            "city": "Glasgow",
            "region": "Scotland",
            "country": "United Kingdom",
            "zip": listing.get('postcode', ''),
            "latitude": str(listing['latitude']),
            "longitude": str(listing['longitude']),
            "phone": listing.get('phone_e164', ''),
            "website": listing.get('website', ''),
            "google_place_id": listing['place_id']
        }

        if existing_id:
            resp = self.session.post(
                f"{self.base}/wp-json/geodir/v2/places/{existing_id}",
                json=payload
            )
        else:
            resp = self.session.post(
                f"{self.base}/wp-json/geodir/v2/places",
                json=payload
            )

        if not resp.ok:
            print(f"❌ WP API Error ({resp.status_code}): {resp.text}")
            
        resp.raise_for_status()
        return resp.json()['id']

    def _find_by_place_id(self, place_id):
        resp = self.session.get(
            f"{self.base}/wp-json/geodir/v2/places",
            params={"meta_key": "google_place_id", "meta_value": place_id, "per_page": 1}
        )
        if not resp.ok:
            return None
        results = resp.json()
        if isinstance(results, list) and results:
            for r in results:
                if r.get('google_place_id') == place_id:
                    return r['id']
        return None
