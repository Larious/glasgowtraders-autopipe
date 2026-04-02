import requests

class ListingProPublisher:
    def __init__(self, url, username, password):
        self.url = f"{url}/wp-json/wp/v2"
        self.auth = (username, password)

    def publish(self, data):
        # HARDCODED VERIFIED IDs
        CAT_ID = 22      # From your screenshot (Plumber)
        LOC_ID = 26842   # From your search (Glasgow)
        
        payload = {
            "title": data['name'],
            "content": f"Professional {data.get('category')} services in Glasgow.",
            "status": "publish",
            "type": "listing",
            # We send these twice: once in the standard taxonomies, once in meta
            "listing-category": [CAT_ID],
            "listing-location": [LOC_ID],
            "meta": {
                "lp_listingpro_options": "on",
                "phone": data.get('phone_number', ''),
                "website": data.get('website', ''),
                "gAddress": data.get('formatted_address', ''), # REQUIRED FOR MAP
                "latitude": data.get('lat', ''),               # REQUIRED FOR MAP
                "longitude": data.get('lng', ''),              # REQUIRED FOR MAP
                "cp_city": "Glasgow",                          # TRiggers the Breadcrumb
                "claimed": "1"
            }
        }

        # IMPORTANT: ListingPro sometimes requires the /listing endpoint
        # rather than the generic /posts endpoint.
        response = requests.post(f"{self.url}/listing", json=payload, auth=self.auth)
        
        if response.status_code in [200, 201]:
            resp = response.json()
            # Handle list vs dict return
            final = resp[0] if isinstance(resp, list) else resp
            print(f"✅ Design Synced! Link: {final.get('link')}")
            return True
        else:
            print(f"❌ Failed: {response.text}")
            return False
