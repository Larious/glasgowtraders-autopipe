#!/usr/bin/env python3
"""Unit tests for the retry/backoff helper in scripts/backfill_place_ids.py.

Run: python3 tests/unit/test_backfill_retry.py
The host drops SSL connections under sustained load (documented trap), so the
backfill's network calls must survive transient ConnectionResetError by retrying
with exponential backoff, and re-raise everything else immediately.
"""
import importlib.util
import os
import ssl
import unittest

import requests

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPT = os.path.join(_HERE, "..", "..", "scripts", "backfill_place_ids.py")
_spec = importlib.util.spec_from_file_location("backfill_place_ids", _SCRIPT)
bpi = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bpi)


class TestRequestWithRetry(unittest.TestCase):
    def test_retries_transient_then_succeeds(self):
        attempts = {"n": 0}

        def op():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise ConnectionResetError("[Errno 54] Connection reset by peer")
            return "ok"

        slept = []
        result = bpi.request_with_retry(
            op, retries=5, base_delay=1.0, sleep=slept.append
        )

        self.assertEqual(result, "ok")
        self.assertEqual(attempts["n"], 3)
        # Exponential backoff between the two retries: 1s then 2s.
        self.assertEqual(slept, [1.0, 2.0])

    def test_raises_after_exhausting_retries(self):
        attempts = {"n": 0}

        def always_fail():
            attempts["n"] += 1
            raise ConnectionResetError("nope")

        with self.assertRaises(ConnectionResetError):
            bpi.request_with_retry(
                always_fail, retries=3, base_delay=1.0, sleep=lambda s: None
            )
        self.assertEqual(attempts["n"], 3)

    def test_retries_requests_connectionerror_and_ssl(self):
        for exc in (requests.exceptions.ConnectionError("reset"),
                    ssl.SSLError("bad handshake"),
                    requests.exceptions.Timeout("slow")):
            attempts = {"n": 0}

            def op(_exc=exc):
                attempts["n"] += 1
                if attempts["n"] < 2:
                    raise _exc
                return "ok"

            result = bpi.request_with_retry(
                op, retries=4, base_delay=0.5, sleep=lambda s: None
            )
            self.assertEqual(result, "ok")
            self.assertEqual(attempts["n"], 2)

    def test_does_not_retry_non_transient(self):
        attempts = {"n": 0}

        def op():
            attempts["n"] += 1
            raise ValueError("programmer error, not a network blip")

        with self.assertRaises(ValueError):
            bpi.request_with_retry(
                op, retries=5, base_delay=1.0, sleep=lambda s: None
            )
        # No retries for a non-network error.
        self.assertEqual(attempts["n"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
