#!/usr/bin/env python3
"""Unit tests for scripts/ledger_utils.py — the shared dedup helper every
publisher must consult before creating a listing (CLAUDE.md rule 3).

Run: python3 tests/unit/test_ledger_utils.py
"""
import importlib.util
import json
import os
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_MOD = os.path.join(_HERE, "..", "..", "scripts", "ledger_utils.py")
_spec = importlib.util.spec_from_file_location("ledger_utils", _MOD)
lu = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lu)


class TestLedgerRoundTrip(unittest.TestCase):
    def test_record_then_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ledger.jsonl")
            lu.record(path, "P_A", 101, "A Ltd", "0141 111", "findplace")
            lu.record(path, "P_B", 102, "B Ltd", "", "csv")
            by_post, by_place = lu.load_ledger(path)
        self.assertEqual(by_post[101]["place_id"], "P_A")
        self.assertEqual(by_place["P_B"]["wp_post_id"], 102)
        self.assertEqual(len(by_post), 2)

    def test_load_missing_file_is_empty(self):
        by_post, by_place = lu.load_ledger("/nonexistent/ledger.jsonl")
        self.assertEqual(by_post, {})
        self.assertEqual(by_place, {})

    def test_load_skips_corrupt_lines(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ledger.jsonl")
            with open(path, "w") as f:
                f.write(json.dumps({"place_id": "P_A", "wp_post_id": 1,
                                    "name": "A"}) + "\n")
                f.write("not json\n")
            by_post, by_place = lu.load_ledger(path)
        self.assertEqual(len(by_post), 1)


class TestExistingPostFor(unittest.TestCase):
    def test_returns_post_id_on_hit_none_on_miss(self):
        by_place = {"P_A": {"place_id": "P_A", "wp_post_id": 55}}
        self.assertEqual(lu.existing_post_for(by_place, "P_A"), 55)
        self.assertIsNone(lu.existing_post_for(by_place, "P_ZZZ"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
