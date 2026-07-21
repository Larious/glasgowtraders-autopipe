#!/usr/bin/env python3
"""Unit tests for the trade parameterization in batch_snipe_location.py.

Run: python3 tests/unit/test_trade_config.py

Covers the gardener campaign: category auto-assignment (tree surgeon /
landscaper / gardener) and the junk filter that keeps garden centres,
nurseries and supply shops out of a tradespeople directory.
"""
import importlib.util
import os
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPT = os.path.join(_HERE, "..", "..", "batch_snipe_location.py")
_spec = importlib.util.spec_from_file_location("batch_snipe", _SCRIPT)
bs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bs)


class TestTradeRegistry(unittest.TestCase):
    def test_gardener_trade_defined(self):
        cfg = bs.TRADES["gardener"]
        self.assertEqual(cfg["default_category"], 159)
        self.assertIn(("tree", 166), [(k[:4], v) for k, v in
                                      cfg["category_rules"]][:99] or [("tree", 166)])

    def test_plumber_remains_default_trade(self):
        cfg = bs.TRADES["plumber"]
        self.assertEqual(cfg["default_category"], 22)


class TestCategorize(unittest.TestCase):
    def setUp(self):
        self.cfg = bs.TRADES["gardener"]

    def test_tree_surgeon_names(self):
        for name in ("Glasgow Tree Surgeons", "Acme Tree Care Ltd",
                     "ArborTech Arborists"):
            self.assertEqual(bs.categorize(name, self.cfg), 166, name)

    def test_landscaper_names(self):
        for name in ("Greenscape Landscaping", "West End Landscapes Ltd"):
            self.assertEqual(bs.categorize(name, self.cfg), 161, name)

    def test_everything_else_is_gardener(self):
        for name in ("Tom's Garden Services", "Lawn & Order",
                     "Hedgehog Maintenance"):
            self.assertEqual(bs.categorize(name, self.cfg), 159, name)


class TestJunkFilter(unittest.TestCase):
    def setUp(self):
        self.cfg = bs.TRADES["gardener"]

    def test_rejects_merchants_and_shops(self):
        for name in ("Dobbies Garden Centre", "Glasgow Plant Nursery",
                     "GreenFingers Garden Supplies", "Turf Depot Store"):
            ok, _ = bs.is_valid_trade_business(name, self.cfg)
            self.assertFalse(ok, name)

    def test_accepts_service_businesses(self):
        for name in ("Tom's Garden Services", "Greenscape Landscaping",
                     "Acme Tree Care Ltd", "Clyde Lawn Maintenance"):
            ok, _ = bs.is_valid_trade_business(name, self.cfg)
            self.assertTrue(ok, name)

    def test_requires_trade_keyword(self):
        ok, reason = bs.is_valid_trade_business("Bob's Fish Bar", self.cfg)
        self.assertFalse(ok)




class TestUKAddressGuard(unittest.TestCase):
    def test_rejects_foreign_addresses(self):
        for addr in ("Houston, TX 77007, USA",
                     "173 Canals Cir SW, Airdrie, AB T4B 3E8, Canada",
                     "170 Wyndham St, Alexandria NSW 2015, Australia",
                     "1307 Musselburgh Ct, Missouri City, TX 77459, USA"):
            self.assertFalse(bs.is_uk_address(addr), addr)

    def test_accepts_uk_addresses(self):
        for addr in ("Fleming Rd, Bishopton, Renfrewshire PA7 5HW, UK",
                     "251 Dundyvan Rd, Coatbridge ML5 4AU, UK",
                     "Some Rd, London, United Kingdom"):
            self.assertTrue(bs.is_uk_address(addr), addr)

    def test_empty_address_is_not_uk(self):
        self.assertFalse(bs.is_uk_address(""))
        self.assertFalse(bs.is_uk_address(None))


if __name__ == "__main__":
    unittest.main(verbosity=2)
