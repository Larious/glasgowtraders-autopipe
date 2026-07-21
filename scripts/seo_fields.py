#!/usr/bin/env python3
"""seo_fields.py — Yoast focus keyphrase + meta description for listings.

Strategy (see the SEO decision on 2026-07-21):
- Focus keyphrase = the business name. Each listing owns a unique phrase, so
  listings don't cannibalise each other or the trade/location pages.
- Meta description: unique, specific, <=156 chars, built from real data
  (trade, town, service area, Google rating). Deterministic per business so
  re-runs don't churn. Written to read plainly — no marketing-AI filler.
"""
import hashlib
import html
import re

MAX_LEN = 156


def focus_keyphrase(name):
    kp = html.unescape(name or "").strip()
    return kp.rstrip(" .,-–—")


def _pick(name, options):
    """Deterministic choice keyed on the business name."""
    h = int(hashlib.md5(name.encode("utf-8")).hexdigest(), 16)
    return options[h % len(options)]


def meta_description(name, trade, town, area="", rating=0, reviews=0):
    name = html.unescape(name or "").strip()
    trade_l = (trade or "tradesperson").lower()
    town = town or "Glasgow"
    # Where they work: prefer a specific area distinct from the town.
    where = town
    if area and area.lower() not in town.lower():
        where = f"{area}, {town}"

    has_rating = rating and reviews

    if has_rating:
        rating_str = f"{rating:g}"
        templates = [
            f"{name} is a {trade_l} in {where}, rated {rating_str}/5 from "
            f"{reviews} Google reviews. See phone, opening hours and location.",
            f"{name} — {trade_l} covering {where}. {rating_str}/5 across "
            f"{reviews} reviews. Find contact details, hours and service area.",
            f"Contact {name}, a {trade_l} in {where} rated {rating_str}/5 by "
            f"{reviews} customers. Phone number, hours and website inside.",
        ]
    else:
        templates = [
            f"{name} is a {trade_l} serving {where} and nearby areas of "
            f"Glasgow. Find phone number, opening hours and location.",
            f"{name} — {trade_l} in {where}. See their phone number, website, "
            f"opening hours and service area on Glasgow Trader.",
            f"Looking for a {trade_l} in {where}? {name} lists phone number, "
            f"opening hours, website and service area here.",
        ]

    md = _pick(name, templates)
    if len(md) <= MAX_LEN:
        return md

    # Long business name blew the budget — step down through progressively
    # more compact forms, each a complete sentence (never a mid-word trim).
    if has_rating:
        candidates = [
            f"{name}: {trade_l} in {town}, rated {rating:g}/5 from {reviews} Google reviews.",
            f"{name}: {trade_l} in {town}, rated {rating:g}/5 ({reviews} reviews).",
            f"{name} — {trade_l} in {town}. Rated {rating:g}/5 on Google.",
            f"{name} — {trade_l} in {town}.",
        ]
    else:
        candidates = [
            f"{name}: {trade_l} in {town}. Phone, opening hours and service area on Glasgow Trader.",
            f"{name}: {trade_l} in {town}. Contact details and opening hours inside.",
            f"{name} — {trade_l} in {town}.",
        ]
    for md in candidates:
        if len(md) <= MAX_LEN:
            return md

    # Pathologically long name: trim the name itself, keep a clean tail.
    tail = f"… — {trade_l} in {town}."
    room = MAX_LEN - len(tail)
    trimmed = name[:room]
    return trimmed[:trimmed.rfind(" ")].rstrip(" .,-–—") + tail


def town_from_address(addr, fallback="Glasgow"):
    """Best-effort town: the segment before the UK postcode."""
    if not addr:
        return fallback
    parts = [p.strip() for p in addr.split(",") if p.strip()]
    # Drop a trailing "UK"/"United Kingdom"
    parts = [p for p in parts if p.upper() not in ("UK", "UNITED KINGDOM")]
    if not parts:
        return fallback
    # Last part often "Town POSTCODE" — strip the postcode token(s).
    tail = re.sub(r"\b[A-Z]{1,2}\d[A-Z\d]?\s*\d?[A-Z]{0,2}\b", "", parts[-1]).strip()
    return tail or fallback
