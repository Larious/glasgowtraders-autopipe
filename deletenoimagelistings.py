import os
import sys
import time
import requests
from requests.auth import HTTPBasicAuth

WPBASEURL     = "https://www.glasgowtrader.co.uk"
WPUSER        = "Dev"
WPAPPPASSWORD = "vMKQMNmn7OQ0 MzkVqalj 6aZv"
AUTH = HTTPBasicAuth(WPUSER, WPAPPPASSWORD)

def main():
    dryrun = "--dry-run" in sys.argv
    print("=" * 60)
    print("GLASGOW TRADERS - Delete No-Image Listings")
    print(f"Mode: {'DRY RUN' if dryrun else 'LIVE DELETE'}")
    print("=" * 60)

    page          = 1
    total_checked = 0
    deleted_count = 0
    failed_count  = 0

    print("Scanning all published listings for missing images...")

    while True:
        resp = requests.get(
            f"{WPBASEURL}/wp-json/wp/v2/listing",
            params={"per_page": 100, "page": page, "status": "publish"},
            auth=AUTH,
            timeout=30,
        )
        if resp.status_code == 400:
            break
        if resp.status_code != 200:
            print(f"Error fetching page {page}: {resp.status_code}")
            break

        listings = resp.json()
        if not listings:
            break

        for listing in listings:
            total_checked += 1
            post_id        = listing["id"]
            title          = listing.get("title", {}).get("rendered", "Unknown")
            featured_media = listing.get("featured_media", 0)

            if featured_media == 0:
                if dryrun:
                    print(f"  DRY RUN  | Would delete → Post {post_id}: {title}")
                    deleted_count += 1
                else:
                    del_resp = requests.delete(
                        f"{WPBASEURL}/wp-json/wp/v2/listing/{post_id}?force=true",
                        auth=AUTH,
                        timeout=15,
                    )
                    if del_resp.status_code == 200:
                        print(f"  DELETED  | Post {post_id}: {title}")
                        deleted_count += 1
                    else:
                        print(f"  FAILED   | Post {post_id}: {title} — Status {del_resp.status_code}")
                        failed_count += 1
                    time.sleep(0.3)

        page += 1

    print("=" * 60)
    print("CLEAN-UP COMPLETE")
    print(f"  Total listings checked : {total_checked}")
    print(f"  {'Would delete' if dryrun else 'Deleted'}         : {deleted_count}")
    if not dryrun:
        print(f"  Failed                 : {failed_count}")
    print("=" * 60)
    if dryrun:
        print("DRY RUN complete. Run without --dry-run to delete for real.")

if __name__ == "__main__":
    main()
