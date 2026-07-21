#!/usr/bin/env python3
"""Unit tests for scripts/seo_fields.py — Yoast focus keyphrase + meta
description generation.

Run: python3 tests/unit/test_seo_fields.py
"""
import importlib.util
import os
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_MOD = os.path.join(_HERE, "..", "..", "scripts", "seo_fields.py")
_spec = importlib.util.spec_from_file_location("seo_fields", _MOD)
seo = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(seo)

BIZ = dict(name="MIG Garden Care", trade="Gardener", town="Glasgow",
           area="Dennistoun", rating=4.8, reviews=27)


class TestFocusKeyphrase(unittest.TestCase):
    def test_keyphrase_is_business_name_decoded(self):
        self.assertEqual(seo.focus_keyphrase("MIG Garden Care"), "MIG Garden Care")
        # HTML entities from WP titles are decoded
        self.assertEqual(seo.focus_keyphrase("A &#038; D Joinery"),
                         "A & D Joinery")

    def test_keyphrase_strips_trailing_punctuation(self):
        self.assertEqual(seo.focus_keyphrase("Glesga Garden Guy Ltd."),
                         "Glesga Garden Guy Ltd")


class TestMetaDescription(unittest.TestCase):
    def test_contains_name_and_town_and_within_limit(self):
        md = seo.meta_description(**BIZ)
        self.assertIn("MIG Garden Care", md)
        self.assertIn("Glasgow", md)
        self.assertLessEqual(len(md), 156)
        self.assertGreaterEqual(len(md), 100)

    def test_includes_rating_when_reviews_present(self):
        md = seo.meta_description(**BIZ)
        self.assertIn("4.8", md)

    def test_omits_rating_clause_when_no_reviews(self):
        b = dict(BIZ, rating=0, reviews=0)
        md = seo.meta_description(**b)
        self.assertNotIn("/5", md)
        self.assertLessEqual(len(md), 156)
        self.assertIn("MIG Garden Care", md)

    def test_long_business_name_still_within_limit(self):
        b = dict(BIZ, name="Landscape Gardeners Glasgow - Decking, Driveways, "
                          "Fencing & Patios Specialists")
        md = seo.meta_description(**b)
        self.assertLessEqual(len(md), 156)

    def test_deterministic(self):
        self.assertEqual(seo.meta_description(**BIZ),
                         seo.meta_description(**BIZ))

    def test_no_ai_tell_words(self):
        tells = ["nestled", "look no further", "trusted partner",
                 "one-stop", "top-notch", "boasts", "when it comes to",
                 "in the heart of", "unrivalled", "bespoke solutions"]
        for b in (BIZ, dict(BIZ, rating=0, reviews=0),
                  dict(BIZ, trade="Tree Surgeon", area="Rutherglen")):
            md = seo.meta_description(**b).lower()
            for t in tells:
                self.assertNotIn(t, md)


if __name__ == "__main__":
    unittest.main(verbosity=2)
