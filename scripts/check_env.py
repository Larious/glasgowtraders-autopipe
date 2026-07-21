#!/usr/bin/env python3
"""Report which required environment variables are configured, WITHOUT
printing any values. Safe for the agent to run any time."""
import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("note: python-dotenv not installed; reading shell env only")

CHECKS = [
    ("WP_BASE_URL",           ["WP_BASE_URL"]),
    ("WP_USERNAME",           ["WP_USERNAME", "WP_USER"]),
    ("WP_APP_PASSWORD",       ["WP_APP_PASSWORD"]),
    ("GOOGLE_PLACES_API_KEY", ["GOOGLE_PLACES_API_KEY", "GOOGLE_API_KEY"]),
]

missing = 0
for label, names in CHECKS:
    hit = next((n for n in names if os.getenv(n)), None)
    if hit:
        extra = "" if hit == names[0] else f" (via legacy alias {hit})"
        print(f"  SET      {label}{extra}")
    else:
        print(f"  MISSING  {label}  (accepted names: {', '.join(names)})")
        missing += 1

base = os.getenv("WP_BASE_URL", "")
if base and "glasgowtraders" in base:
    print("  WARNING  WP_BASE_URL contains 'glasgowtraders' — the domain has no 's'.")
    missing += 1
if base and base.rstrip("/").endswith("glasgowtrader.co.uk") and "//www." not in base:
    print("  WARNING  WP_BASE_URL should use the www host: https://www.glasgowtrader.co.uk")

sys.exit(1 if missing else 0)
