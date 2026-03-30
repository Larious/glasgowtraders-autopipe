import googlemaps
import time
import redis
import re
from dataclasses import dataclass
from typing import Optional

GLASGOW_CENTRE = (55.8642, -4.2518)
GLASGOW_POSTCODE_RE = re.compile(r'^G\d{1,2}\s?\d[A-Z]{2}$', re.I)

@dataclass
class RawBusiness:
    place_id: str
    name: str
    address: str
    postcode: Optional[str]
    lat: float
    lng: float
    phone: Optional[str]
    website: Optional[str]
    rating: Optional[float]
    review_count: Optional[int]
    categories: list[str]
    hours: dict
    status: str  # OPERATIONAL | CLOSED_PERMANENTLY

class GlasgowPlacesClient:
    def __init__(self, api_key: str, redis_client: redis.Redis):
        self.gmaps = googlemaps.Client(key=api_key)
        self.redis = redis_client

    def search_category(self, category: str) -> list[RawBusiness]:
        results, token = [], None
        while True:
            if token:
                time.sleep(2)
            resp = self.gmaps.places_nearby(
                location=GLASGOW_CENTRE,
                radius=12000,
                type=category,
                page_token=token
            )
            for r in resp.get("results", []):
                detail = self._get_detail(r["place_id"])
                if self._is_glasgow(detail):
                    results.append(self._parse(detail))
            token = resp.get("next_page_token")
            if not token:
                break
        return results

    def _is_glasgow(self, detail: dict) -> bool:
        postcode = self._extract_postcode(detail)
        return bool(postcode and GLASGOW_POSTCODE_RE.match(postcode))

    def _extract_postcode(self, detail: dict) -> Optional[str]:
        address = detail.get("formatted_address", "")
        match = re.search(r'G\d{1,2}\s?\d[A-Z]{2}', address, re.I)
        return match.group(0) if match else None

    def _get_detail(self, place_id: str) -> dict:
        cached = self.redis.get(f"place:{place_id}")
        if cached:
            return eval(cached)
        detail = self.gmaps.place(place_id, fields=[
            "name","formatted_address","formatted_phone_number",
            "website","geometry","rating","user_ratings_total",
            "type","opening_hours","business_status"
        ])["result"]
        self.redis.setex(f"place:{place_id}", 86400, str(detail))
        return detail

    def _parse(self, detail: dict) -> RawBusiness:
        return RawBusiness(
            place_id=detail.get("place_id", ""),
            name=detail.get("name", ""),
            address=detail.get("formatted_address", ""),
            postcode=self._extract_postcode(detail),
            lat=detail["geometry"]["location"]["lat"],
            lng=detail["geometry"]["location"]["lng"],
            phone=detail.get("formatted_phone_number"),
            website=detail.get("website"),
            rating=detail.get("rating"),
            review_count=detail.get("user_ratings_total"),
            categories=detail.get("type", []),
            hours=detail.get("opening_hours", {}),
            status=detail.get("business_status", "OPERATIONAL"),
        )
