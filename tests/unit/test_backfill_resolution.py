#!/usr/bin/env python3
"""Unit tests for title cleaning and dry-run duplicate detection in
scripts/backfill_place_ids.py.

Run: python3 tests/unit/test_backfill_resolution.py

Covers two defects found in the 2026-07-21 full dry-run:
1. WP rendered titles carry HTML entities (&#038;, &#8211;) which pollute both
   the Find Place query and norm_name similarity scoring.
2. Dry-run never populates the in-run place->post map, so DUPLICATE_OF can
   never appear in the plan preview even when two posts share a place_id.
"""
import csv
import importlib.util
import os
import sys
import tempfile
import unittest
from unittest import mock

from requests.auth import HTTPBasicAuth

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPT = os.path.join(_HERE, "..", "..", "scripts", "backfill_place_ids.py")
_spec = importlib.util.spec_from_file_location("backfill_place_ids", _SCRIPT)
bpi = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bpi)


class TestCleanTitle(unittest.TestCase):
    def test_unescapes_html_entities(self):
        self.assertEqual(bpi.clean_title("Taylor Plumbing &#038; Heating"),
                         "Taylor Plumbing & Heating")
        self.assertEqual(bpi.clean_title("PR Electrics &#8211; Cumbernauld"),
                         "PR Electrics – Cumbernauld")
        self.assertEqual(bpi.clean_title("Plain Name Ltd"), "Plain Name Ltd")
        self.assertEqual(bpi.clean_title(""), "")

    def test_norm_name_after_cleaning_drops_entity_digits(self):
        # The bug: without cleaning, "038" survives into the normalized name.
        self.assertEqual(bpi.norm_name(bpi.clean_title("A &#038; B Joinery")),
                         "abjoinery")


class TestDryRunDuplicateDetection(unittest.TestCase):
    def test_dry_run_marks_second_post_with_same_place_id_duplicate(self):
        listings = [
            {"id": 101, "title": {"rendered": "A Ltd"}},
            {"id": 102, "title": {"rendered": "A Ltd"}},
        ]
        cand = {"place_id": "PLACE_A", "name": "A Ltd",
                "formatted_address": "1 Street, Glasgow"}

        tmp = tempfile.mkdtemp()
        cwd = os.getcwd()
        try:
            os.chdir(tmp)
            with mock.patch.object(bpi, "fetch_all_listings",
                                   return_value=listings), \
                 mock.patch.object(bpi, "fetch_listing_meta", return_value={}), \
                 mock.patch.object(bpi, "find_place", return_value=cand), \
                 mock.patch.object(bpi, "AUTH", HTTPBasicAuth("u", "p")), \
                 mock.patch.object(bpi.time, "sleep"), \
                 mock.patch.object(sys, "argv", ["backfill_place_ids.py"]):
                bpi.main()

            with open("data/place_backfill_plan.csv", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        finally:
            os.chdir(cwd)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["post_id"], "101")
        self.assertEqual(rows[0]["source"], "findplace")
        self.assertEqual(rows[1]["post_id"], "102")
        self.assertEqual(rows[1]["source"], "DUPLICATE_OF")
        self.assertEqual(rows[1]["detail"], "101")


if __name__ == "__main__":
    unittest.main(verbosity=2)
