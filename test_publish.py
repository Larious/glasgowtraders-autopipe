from publisher.wordpress_client import WordPressPublisher
from dotenv import load_dotenv
import os

load_dotenv()

pub = WordPressPublisher(
    os.getenv("WP_BASE_URL"),
    os.getenv("WP_USERNAME"),
    os.getenv("WP_APP_PASSWORD")
)

test_listing = {
    "place_id": "ChIJtest123",
    "name": "Test Plumber Glasgow",
    "address": "1 Sauchiehall St, Glasgow G2 3AB",
    "postcode": "G2 3AB",
    "latitude": 55.8642,
    "longitude": -4.2518,
    "phone_e164": "+441412345678",
    "website": "https://example.com",
    "health_score": 80,
}

post_id = pub.publish(test_listing)
print("Published! WordPress post ID: " + str(post_id))
