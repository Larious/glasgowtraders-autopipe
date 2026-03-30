import requests
from requests.auth import HTTPBasicAuth

class WordPressPublisher:
    def __init__(self, base_url, username, app_password):
        self.base = base_url.rstrip("/")
        self.auth = HTTPBasicAuth(username, app_password)
        self.session = requests.Session()
        self.session.auth = self.auth

    def publish(self, listing: dict) -> int:
        """Upsert listing. Returns WordPress post ID."""
        existing_id = self._find_by_place_id(listing["place_id"])

        payload = {
            "title":  listing["name"],
            "status": "publish",
            "geodir_email":     "",
            "geodir_phone":     listing.get("phone_e164", ""),
            "geodir_website":   listing.get("website", ""),
            "geodir_latitude":  str(listing["latitude"]),
            "geodir_longitude": str(listing["longitude"]),
            "geodir_zip":       listing["postcode"],
            "geodir_address":   listing["address"],
            "health_score":     listing["health_score"],
            "google_place_id":  listing["place_id"],
        }

        if existing_id:
            resp = self.session.patch(
                f"{self.base}/wp-json/geodir/v2/listings/{existing_id}",
                json=payload
            )
        else:
            resp = self.session.post(
                f"{self.base}/wp-json/geodir/v2/listings",
                json=payload
            )

        resp.raise_for_status()
        return resp.json()["id"]

    def _find_by_place_id(self, place_id: str):
        resp = self.session.get(
            f"{self.base}/wp-json/geodir/v2/listings",
            params={"meta_key": "google_place_id", "meta_value": place_id}
        )
        results = resp.json()
        return results[0]["id"] if results else None
```

Save and exit. Then commit:
```
git add -A
git commit -m "phase-05: WordPress REST API publisher with idempotent upserts"
git push
