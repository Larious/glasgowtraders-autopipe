#!/usr/bin/env python3
"""Unit tests for durable place_id storage (bridge v2.1 contract).

Run: python3 tests/unit/test_backfill_durable_meta.py

Background (runs/backfill_live_20260721.md): custom keys inside
lp_listingpro_options get wiped server-side within minutes. Bridge v2.1
mirrors google_place_id to standalone post meta `gt_google_place_id` and
echoes it in `verified`. The script must (a) prefer the standalone key when
resolving, and (b) refuse to count a write as verified unless the standalone
echo matches — so running against an undeployed v2.0 bridge fails loudly
instead of silently losing writes.
"""
import importlib.util
import os
import unittest
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPT = os.path.join(_HERE, "..", "..", "scripts", "backfill_place_ids.py")
_spec = importlib.util.spec_from_file_location("backfill_place_ids", _SCRIPT)
bpi = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bpi)


class TestExtractPlaceId(unittest.TestCase):
    def test_prefers_standalone_meta(self):
        meta = {
            "gt_google_place_id": {"value": "PLACE_STANDALONE"},
            "lp_listingpro_options": {"value": {"google_place_id": "PLACE_LP"}},
        }
        self.assertEqual(bpi.extract_place_id(meta), "PLACE_STANDALONE")

    def test_falls_back_to_lp_options(self):
        meta = {
            "lp_listingpro_options": {"value": {"google_place_id": "PLACE_LP"}},
        }
        self.assertEqual(bpi.extract_place_id(meta), "PLACE_LP")

    def test_empty_when_absent(self):
        self.assertEqual(bpi.extract_place_id({}), "")
        self.assertEqual(
            bpi.extract_place_id({"lp_listingpro_options": {"value": {}}}), "")


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class TestWritePlaceIdVerification(unittest.TestCase):
    def test_verified_requires_standalone_echo(self):
        # v2.1 bridge echoes both keys -> success
        ok = _FakeResponse(200, {"verified": {
            "google_place_id": "P1", "gt_google_place_id": "P1"}})
        with mock.patch.object(bpi.session, "post", return_value=ok):
            self.assertTrue(bpi.write_place_id(1, "P1"))

    def test_old_bridge_without_standalone_echo_fails(self):
        # v2.0 bridge echoes only the lp-options key -> must NOT count as
        # verified (write would be wiped server-side).
        old = _FakeResponse(200, {"verified": {"google_place_id": "P1"}})
        with mock.patch.object(bpi.session, "post", return_value=old):
            self.assertFalse(bpi.write_place_id(1, "P1"))

    def test_mismatched_echo_fails(self):
        bad = _FakeResponse(200, {"verified": {
            "google_place_id": "P1", "gt_google_place_id": "OTHER"}})
        with mock.patch.object(bpi.session, "post", return_value=bad):
            self.assertFalse(bpi.write_place_id(1, "P1"))

    def test_http_error_fails(self):
        with mock.patch.object(bpi.session, "post",
                               return_value=_FakeResponse(500, {})):
            self.assertFalse(bpi.write_place_id(1, "P1"))




class TestCacheBusting(unittest.TestCase):
    """LiteSpeed caches authenticated /wp-json/ GETs (x-litespeed-cache: hit),
    serving pre-write state. Every bridge read must carry a cache-buster."""

    def test_fetch_listing_meta_sends_cache_buster(self):
        captured = {}

        def fake_get(url, **kw):
            captured.update(kw)
            return _FakeResponse(200, {"meta": {"k": {"value": 1}}})

        with mock.patch.object(bpi.session, "get", side_effect=fake_get):
            meta = bpi.fetch_listing_meta(42)
        self.assertEqual(meta, {"k": {"value": 1}})
        params = captured.get("params") or {}
        self.assertIn("nocache", params)
        self.assertTrue(params["nocache"])

class TestCacheBusting(unittest.TestCase):
    """LiteSpeed caches authenticated /wp-json/ GETs (x-litespeed-cache: hit),
    serving pre-write state. Every bridge read must carry a cache-buster."""

    def test_fetch_listing_meta_sends_cache_buster(self):
        captured = {}

        def fake_get(url, **kw):
            captured.update(kw)
            return _FakeResponse(200, {"meta": {"k": {"value": 1}}})

        with mock.patch.object(bpi.session, "get", side_effect=fake_get):
            meta = bpi.fetch_listing_meta(42)
        self.assertEqual(meta, {"k": {"value": 1}})
        params = captured.get("params") or {}
        self.assertIn("nocache", params)
        self.assertTrue(params["nocache"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
