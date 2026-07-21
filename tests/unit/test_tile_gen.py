#!/usr/bin/env python3
"""Smoke tests for scripts/tile_gen.py — the per-business featured tile.

Run: python3 tests/unit/test_tile_gen.py
"""
import importlib.util
import os
import tempfile
import unittest

from PIL import Image

_HERE = os.path.dirname(os.path.abspath(__file__))
_MOD = os.path.join(_HERE, "..", "..", "scripts", "tile_gen.py")
_spec = importlib.util.spec_from_file_location("tile_gen", _MOD)
tg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tg)


class TestTile(unittest.TestCase):
    def test_produces_valid_jpeg_of_expected_size(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = tg.make_business_tile("Thistle Garden Maintenance", 159,
                                      "Glasgow", os.path.join(tmp, "t.jpg"))
            self.assertTrue(os.path.exists(p))
            with Image.open(p) as im:
                self.assertEqual(im.size, (tg.W, tg.H))
                self.assertEqual(im.format, "JPEG")

    def test_very_long_name_still_renders(self):
        with tempfile.TemporaryDirectory() as tmp:
            name = ("Landscape Gardeners Glasgow - Decking, Driveways, "
                    "Fencing & Patios Specialists Limited")
            p = tg.make_business_tile(name, 161, "Glasgow",
                                      os.path.join(tmp, "t.jpg"))
            with Image.open(p) as im:
                self.assertEqual(im.size, (tg.W, tg.H))

    def test_unknown_category_uses_default_style(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = tg.make_business_tile("Some Business", 99999, "Paisley",
                                      os.path.join(tmp, "t.jpg"))
            self.assertTrue(os.path.exists(p))

    def test_decodes_html_entities_in_name(self):
        # should not raise on entity-laden names
        with tempfile.TemporaryDirectory() as tmp:
            tg.make_business_tile("A &#038; D Joinery", 159, "Glasgow",
                                  os.path.join(tmp, "t.jpg"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
