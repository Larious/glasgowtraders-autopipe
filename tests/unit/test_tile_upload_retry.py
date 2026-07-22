#!/usr/bin/env python3
"""Regression tests for build_and_upload_tile retry.

46 listings (Solar 33, Scaffolders 7, CCTV 6) published without a featured
image because the WP media endpoint returned an intermittent 500/400 and the
uploader gave up after one attempt. These lock in the retry.
"""
import importlib.util
import os
import sys
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

_spec = importlib.util.spec_from_file_location(
    "batch_snipe", os.path.join(ROOT, "batch_snipe_location.py"))
bs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bs)


def resp(status, body=None):
    r = mock.Mock()
    r.status_code = status
    r.json.return_value = body or {}
    return r


ENRICHED = {"name": "Acme Solar Ltd", "formatted_address": "1 Main St, Glasgow G1 1AA, UK"}


class TestTileUploadRetry(unittest.TestCase):
    def setUp(self):
        # Don't touch the filesystem or draw a real tile.
        p1 = mock.patch.object(bs.tile_gen, "make_business_tile")
        p2 = mock.patch("builtins.open", mock.mock_open(read_data=b"\xff\xd8jpeg"))
        p3 = mock.patch.object(bs.time, "sleep")
        p4 = mock.patch.object(bs.os, "makedirs")
        for p in (p1, p2, p3, p4):
            p.start(); self.addCleanup(p.stop)

    def test_succeeds_after_transient_500(self):
        """A 500 on the first attempt must not lose the featured image."""
        with mock.patch.object(bs.session, "post",
                               side_effect=[resp(500), resp(201, {"id": 991}), resp(200)]) as post:
            self.assertEqual(build(), 991)
        self.assertGreaterEqual(post.call_count, 2)

    def test_succeeds_after_transient_400(self):
        with mock.patch.object(bs.session, "post",
                               side_effect=[resp(400), resp(400), resp(201, {"id": 7}), resp(200)]):
            self.assertEqual(build(), 7)

    def test_retries_connection_errors(self):
        err = bs.requests.exceptions.ConnectionError("reset by peer")
        with mock.patch.object(bs.session, "post",
                               side_effect=[err, resp(201, {"id": 42}), resp(200)]):
            self.assertEqual(build(), 42)

    def test_gives_up_after_all_tries(self):
        """Persistent failure returns None rather than raising."""
        with mock.patch.object(bs.session, "post", side_effect=[resp(500)] * 4) as post:
            self.assertIsNone(build())
        self.assertEqual(post.call_count, 4)

    def test_first_attempt_success_does_not_retry(self):
        with mock.patch.object(bs.session, "post",
                               side_effect=[resp(201, {"id": 5}), resp(200)]) as post:
            self.assertEqual(build(), 5)
        self.assertEqual(post.call_count, 2)  # upload + alt_text

    def test_backoff_is_exponential(self):
        with mock.patch.object(bs.session, "post",
                               side_effect=[resp(500), resp(500), resp(201, {"id": 1}), resp(200)]):
            build()
        waits = [c.args[0] for c in bs.time.sleep.call_args_list if c.args]
        self.assertIn(1, waits)
        self.assertIn(2, waits)


def build():
    return bs.build_and_upload_tile(ENRICHED, 305, "Glasgow")


if __name__ == "__main__":
    unittest.main(verbosity=2)
