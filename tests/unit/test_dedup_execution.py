#!/usr/bin/env python3
"""Unit tests for scripts/apply_resolutions.py and bulk_delete_listings.py
(CSV-driven rewrite).

Run: python3 tests/unit/test_dedup_execution.py
"""
import csv
import importlib.util
import json
import os
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))


def _load(relpath, name):
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(_HERE, "..", "..", relpath))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestApplyResolutions(unittest.TestCase):
    def setUp(self):
        self.ar = _load("scripts/apply_resolutions.py", "apply_resolutions")

    def test_build_ops_accepts_only_accept_rows_not_already_ledgered(self):
        accepts = [
            {"post_id": "1", "title": "A", "place_id": "P_A", "decision": "ACCEPT"},
            {"post_id": "2", "title": "B", "place_id": "P_B", "decision": "REJECT"},
            {"post_id": "3", "title": "C", "place_id": "P_C", "decision": "ACCEPT"},
        ]
        ledger_by_post = {3: {"place_id": "P_C", "wp_post_id": 3}}
        ops = self.ar.build_ops(accepts, {}, {}, ledger_by_post)
        kinds = [(o["op"], o["post_id"]) for o in ops]
        self.assertIn(("write_place_id", 1), kinds)
        self.assertNotIn(("write_place_id", 2), kinds)   # rejected
        self.assertNotIn(("write_place_id", 3), kinds)   # already ledgered

    def test_build_ops_keeper_moves_and_locations(self):
        moves = {"P_X": {"from_post": 10, "to_post": 20, "name": "X Ltd"}}
        locs = {"20": {"term_name": "Airdrie", "term_id": 173}}
        ops = self.ar.build_ops([], moves, locs, {10: {"place_id": "P_X"}})
        kinds = [(o["op"], o["post_id"]) for o in ops]
        self.assertIn(("write_place_id", 20), kinds)
        self.assertIn(("move_ledger", 20), kinds)
        self.assertIn(("add_location", 20), kinds)
        loc_op = [o for o in ops if o["op"] == "add_location"][0]
        self.assertEqual(loc_op["term_id"], 173)

    def test_rewrite_ledger_entry_moves_post_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ledger.jsonl")
            with open(path, "w") as f:
                f.write(json.dumps({"place_id": "P_X", "wp_post_id": 10,
                                    "name": "old"}) + "\n")
                f.write(json.dumps({"place_id": "P_Y", "wp_post_id": 11,
                                    "name": "other"}) + "\n")
            self.ar.rewrite_ledger_entry(path, "P_X", 20, "X Ltd")
            recs = [json.loads(l) for l in open(path)]
        self.assertEqual(len(recs), 2)
        moved = [r for r in recs if r["place_id"] == "P_X"][0]
        self.assertEqual(moved["wp_post_id"], 20)
        self.assertEqual(moved["name"], "X Ltd")
        untouched = [r for r in recs if r["place_id"] == "P_Y"][0]
        self.assertEqual(untouched["wp_post_id"], 11)


class TestBulkDeleteCsv(unittest.TestCase):
    def setUp(self):
        self.bd = _load("bulk_delete_listings.py", "bulk_delete_listings")

    def test_load_trash_ids_only_trash_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "plan.csv")
            with open(path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["group", "post_id", "title", "action", "reason"])
                w.writerow([1, 100, "A", "TRASH", "dupe"])
                w.writerow([1, 101, "B", "KEEP", "keeper"])
                w.writerow([2, 102, "C", "TRASH", "dupe"])
            ids = self.bd.load_trash_ids(path)
        self.assertEqual(ids, [100, 102])

    def test_load_trash_ids_rejects_over_batch_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "plan.csv")
            with open(path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["group", "post_id", "title", "action", "reason"])
                for i in range(26):   # over the ≤25 per-run rule
                    w.writerow([1, 100 + i, "X", "TRASH", "d"])
            with self.assertRaises(SystemExit):
                self.bd.load_trash_ids(path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
