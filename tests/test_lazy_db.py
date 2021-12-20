#!/usr/bin/env python

"""Tests for `lazy_db` package."""


import unittest
import os
from lazy_db import lazy_db


class TestLazy_db(unittest.TestCase):
    """Tests for `lazy_db` package."""

    def setUp(self):
        """Set up test db"""
        self.db = lazy_db.LazyDb("test_db.lazydb")

    def tearDown(self):
        """Tear down test fixtures, if any."""
        self.db.close()
        os.remove("test_db.lazydb")

    def test_1_string_insertion(self):
        """Test insertion"""
        self.db.write("test_str", "here is a test value")
        value = self.db.read("test_str")
        self.assertEqual("here is a test value", value)

    def test_1_string_insertion2(self):
        """Test insertion"""
        self.db.write("test_str2", "here is a test value2")

    def test_2_string_read(self):
        """Test string read"""
        value = self.db.read("test_str")
        self.assertEqual("here is a test value", value)

    def test_2_string_read2(self):
        """Test string read"""
        value = self.db.read("test_str")
        self.assertEqual("here is a test value2", value)

    def test_3_close_db(self):
        """Close database"""
        self.db.close()

    def test_4_open_db(self):
        """Reopen database"""
        self.db = lazy_db.LazyDb("test_db.lazydb")

    def test_5_second_test_string_read(self):
        """Test string read after database is reopened"""
        value = self.db.read("test_str")
        self.assertEqual("here is a test value", value)

    def test_5_second_test_string_read2(self):
        """Test string read after database is reopened"""
        value = self.db.read("test_str2")
        self.assertEqual("here is a test value2", value)

