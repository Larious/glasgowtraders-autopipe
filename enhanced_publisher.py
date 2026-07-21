#!/usr/bin/env python3
"""
Enhanced bulk publisher that adds:
- Photo downloads from Google Places
- Opening hours
- Better descriptions
"""

import os
import csv
import json
import sys
import requests
from typing import Dict, List, Optional
import time
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "scripts"))
import ledger_utils


# Configuration
WP_BASE_URL = os.getenv('WP_BASE_URL', 'https://www.glasgowtrader.co.uk')
WP_USERNAME = os.getenv('WP_USERNAME')
WP_APP_PASSWORD = os.getenv('WP_APP_PASSWORD')
GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
PLUMBERS_CATEGORY_ID = 22


class GooglePlacesPhotoDownloader:
    """Download photos from Google Places API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
    
    def get_place_details(self, place_id: str) -> Optional[Dict]:
        """Fetch full place details including photos and opening hours"""
        url = f"https://places.googleapis.com/v1/places/{place_id}"
        
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': self.api_key,
            'X-Goog-FieldMask': (
                'id,displayName,photos,regularOpeningHours,'
                'editorialSummary,formattedAddress'
            )
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=15)
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"    Error fetching details: {response.status_code}")
                return None
        except Exception as e:
            print(f"    Error: {str(e)}")
            return None
    
    def download_photo(self, photo_name: str, max_width: int = 1200) -> Optional[bytes]:
        """FROZEN — Google ToS: downloading Places photos is a prohibited use
        for this directory (CLAUDE.md rule 5). Images come later via the
        ListingPro claim flow."""
        return None


class EnhancedListingPublisher:
    """Enhanced publisher with photos and opening hours"""
    
    def __init__(self, base_url: str, username: str, app_password: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        self.auth = (username, app_password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.photo_downloader = GooglePlacesPhotoDownloader(api_key)
    
    def check_duplicate(self, google_place_id: str) -> Optional[int]:
        """Check the ledger — the dedup source of truth (rule 3). The old
        REST scan only saw the first 100 posts and an unregistered meta
        field, so it never actually caught anything."""
        _, by_place = ledger_utils.load_ledger()
        return ledger_utils.existing_post_for(by_place, google_place_id)
    
    def upload_photo_to_wordpress(self, photo_bytes: bytes, filename: str) -> Optional[int]:
        """Upload photo to WordPress media library"""
        url = f"{self.base_url}/wp-json/wp/v2/media"
        
        files = {
            'file': (filename, photo_bytes, 'image/jpeg')
        }
        
        response = self.session.post(url, files=files)
        
        if response.status_code == 201:
            media_id = response.json()['id']
            print(f"    ✓ Uploaded photo (media #{media_id})")
            return media_id
        else:
            print(f"    ✗ Photo upload failed: {response.status_code}")
            return None
    
    def create_listing(self, business: Dict) -> Optional[int]:
        """Create listing post"""
        url = f"{self.base_url}/wp-json/wp/v2/listing"
        
        # Better description
        if business.get('editorial_summary'):
            description = business['editorial_summary']
        else:
            # Create a more detailed default description
            description = f"{business['name']} is a professional plumbing service serving {business['location_name']} and the surrounding Glasgow area."
            
            if business['rating'] > 0:
                description += f" They have an excellent {business['rating']}-star rating from {business['user_ratings_total']} customer reviews on Google."
            
            if business['website']:
                description += f" Visit their website for more information about their plumbing services."
        
        post_data = {
            'title': business['name'],
            'content': description,
            'status': 'publish',
            'listing-category': [PLUMBERS_CATEGORY_ID],
            'location': [business['location_id']],
            'meta': {
                'google_place_id': business['google_place_id'],
                'rating': str(business['rating']),
                'user_ratings_total': str(business['user_ratings_total'])
            }
        }
        
        response = self.session.post(url, json=post_data)
        
        if response.status_code == 201:
            post_id = response.json()['id']
            print(f"  ✓ Created listing #{post_id}: {business['name']}")
            return post_id
        else:
            print(f"  ✗ Failed: {response.status_code}")
            return None
    
    def set_listingpro_options(self, post_id: int, business: Dict) -> bool:
        """Set sidebar options including opening hours"""
        url = f"{self.base_url}/wp-json/glasgow-traders/v1/listing-options/{post_id}"
        
        options = {
            'phone': business['phone'],
            'gAddress': business['address'],
            'latitude': str(business['latitude']),
            'longitude': str(business['longitude']),
            'website': business['website'],
            'claimed_section': 'not_claimed',
            'reviews_ids': '',
            # Dedup key (rule 3); bridge v2.2 mirrors to standalone meta.
            'google_place_id': business['google_place_id'],
        }
        
        # Add opening hours if available
        if business.get('opening_hours'):
            options['business_hours'] = business['opening_hours']
        
        response = self.session.post(url, json=options)
        
        if response.status_code == 200:
            print(f"  ✓ Set sidebar options for #{post_id}")
            return True
        else:
            return False
    
    def set_featured_image_and_gallery(self, post_id: int, featured_id: int, gallery_ids: str) -> bool:
        """Set featured image and gallery with multiple photos"""
        url = f"{self.base_url}/wp-json/wp/v2/listing/{post_id}"
        
        data = {
            'featured_media': featured_id,
            'meta': {
                'gallery_image_ids': gallery_ids
            }
        }
        
        response = self.session.post(url, json=data)
        return response.status_code == 200
    
    def publish_business(self, business: Dict, dry_run: bool = False) -> bool:
        """Full publishing with photos and hours"""
        
        # Check duplicate
        existing_id = self.check_duplicate(business['google_place_id'])
        if existing_id:
            print(f"⊗ SKIP: {business['name']} (exists as #{existing_id})")
            return False
        
        if dry_run:
            print(f"⊕ WOULD CREATE: {business['name']} in {business['location_name']}")
            return True
        
        # Fetch full place details (photos + opening hours)
        print(f"  → Fetching details from Google...")
        details = self.photo_downloader.get_place_details(business['google_place_id'])
        
        media_ids = []  # Store all uploaded photo IDs
        
        if details:
            # Extract editorial summary
            editorial = details.get('editorialSummary', {}).get('text', '')
            if editorial:
                business['editorial_summary'] = editorial
            
            # Extract opening hours
            opening_hours_data = details.get('regularOpeningHours', {})
            if opening_hours_data:
                business['opening_hours'] = self.parse_opening_hours(opening_hours_data)
            
            # Download up to 5 photos
            photos = details.get('photos', [])
            max_photos = min(5, len(photos))
            
            if max_photos > 0:
                print(f"  → Downloading {max_photos} photo(s)...")
                
                for idx, photo in enumerate(photos[:max_photos], 1):
                    photo_name = photo.get('name')
                    if photo_name:
                        photo_bytes = self.photo_downloader.download_photo(photo_name)
                        
                        if photo_bytes:
                            # Upload to WordPress
                            filename = f"{business['name'].replace(' ', '_')}_photo_{idx}.jpg"
                            media_id = self.upload_photo_to_wordpress(photo_bytes, filename)
                            
                            if media_id:
                                media_ids.append(media_id)
                            
                            time.sleep(0.5)  # Rate limit between photos
        
        time.sleep(1)  # Rate limit
        
        # Create listing
        post_id = self.create_listing(business)
        if not post_id:
            return False

        # Record in the ledger in the same run (rule 3)
        ledger_utils.record(ledger_utils.DEFAULT_LEDGER,
                            business['google_place_id'], post_id,
                            business['name'], business.get('phone', ''), 'csv')

        # Set sidebar options
        self.set_listingpro_options(post_id, business)
        
        # Set featured image and gallery
        if media_ids:
            # First photo is featured image
            featured_id = media_ids[0]
            # All photos go in gallery
            gallery_ids = ','.join(str(mid) for mid in media_ids)
            
            self.set_featured_image_and_gallery(post_id, featured_id, gallery_ids)
            print(f"  ✓ Added {len(media_ids)} photo(s) to gallery")
        
        return True
    
    def parse_opening_hours(self, hours_data: Dict) -> Dict:
        """Parse Google opening hours to ListingPro format"""
        day_map = {
            0: 'Sunday',
            1: 'Monday', 
            2: 'Tuesday',
            3: 'Wednesday',
            4: 'Thursday',
            5: 'Friday',
            6: 'Saturday'
        }
        
        periods = hours_data.get('periods', [])
        weekday_descriptions = hours_data.get('weekdayDescriptions', [])
        
        # 24/7 business: single period, day 0, no close key. Expand to all
        # seven days (never a single Sunday row, never skip).
        if len(periods) == 1 and 'close' not in periods[0]:
            return {day: {'open': '12:00am', 'close': '11:59pm'}
                    for day in day_map.values()}
        
        # Normal hours parsing
        formatted_hours = {}
        
        for period in periods:
            open_day = period.get('open', {}).get('day')
            close_day = period.get('close', {}).get('day')
            
            if open_day is None:
                continue
            
            day_name = day_map.get(open_day)
            if not day_name:
                continue
            
            open_time = period.get('open', {}).get('time', '0000')
            close_time = period.get('close', {}).get('time', '2359') if period.get('close') else '2359'
            
            # Convert 24h to 12h format
            open_12h = self.convert_to_12h(open_time)
            close_12h = self.convert_to_12h(close_time)
            
            formatted_hours[day_name] = {
                'open': open_12h,
                'close': close_12h
            }
        
        return formatted_hours
    
    def convert_to_12h(self, time_24: str) -> str:
        """Convert 24h time to 12h format (e.g., '1400' -> '02:00pm')"""
        hour = int(time_24[:2])
        minute = int(time_24[2:])
        
        period = 'am' if hour < 12 else 'pm'
        if hour == 0:
            hour = 12
        elif hour > 12:
            hour -= 12
        
        return f"{hour:02d}:{minute:02d}{period}"


def load_csv(filename: str) -> List[Dict]:
    """Load plumbers from CSV"""
    plumbers = []
    
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row['latitude'] = float(row['latitude']) if row['latitude'] else 0
            row['longitude'] = float(row['longitude']) if row['longitude'] else 0
            row['rating'] = float(row['rating']) if row['rating'] else 0
            row['user_ratings_total'] = int(row['user_ratings_total']) if row['user_ratings_total'] else 0
            row['location_id'] = int(row['location_id'])
            plumbers.append(row)
    
    return plumbers


def main():
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python3 enhanced_publisher.py <csv_file> [--dry-run]")
        return
    
    csv_file = sys.argv[1]
    dry_run = '--dry-run' in sys.argv
    
    plumbers = load_csv(csv_file)
    print(f"Loaded {len(plumbers)} plumbers\n")
    
    publisher = EnhancedListingPublisher(
        WP_BASE_URL, 
        WP_USERNAME, 
        WP_APP_PASSWORD,
        GOOGLE_PLACES_API_KEY
    )
    
    stats = {'total': len(plumbers), 'published': 0, 'skipped': 0}
    
    for i, plumber in enumerate(plumbers, 1):
        print(f"[{i}/{len(plumbers)}] {plumber['name']} ({plumber['location_name']})")
        
        if publisher.publish_business(plumber, dry_run):
            stats['published'] += 1
        else:
            stats['skipped'] += 1
        
        print()
        time.sleep(3 if not dry_run else 0)
    
    print("=" * 60)
    print(f"Total: {stats['total']} | Published: {stats['published']} | Skipped: {stats['skipped']}")
    print("=" * 60)


if __name__ == '__main__':
    main()
