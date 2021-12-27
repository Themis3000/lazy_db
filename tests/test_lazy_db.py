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

    def restart(self):
        self.db.close()
        self.db = lazy_db.LazyDb("test_db.lazydb")

    def test_string_insertions(self):
        """Test 2 string insertions"""
        self.db.write("test_str", "here is a test value")
        self.db.write("test_str2", "here is a test value2")
        value1 = self.db.read("test_str")
        value2 = self.db.read("test_str2")
        self.assertEqual("here is a test value", value1)
        self.assertEqual("here is a test value2", value2)

    def test_int_insertions(self):
        """test 2 int insertions"""
        self.db.write("test_str", 346735)
        self.db.write("test_str2", 982745)
        value1 = self.db.read("test_str")
        value2 = self.db.read("test_str2")
        self.assertEqual(346735, value1)
        self.assertEqual(982745, value2)

    def test_int_key_insertions(self):
        """test 2 int key insertions"""
        self.db.write(43556, 346735)
        self.db.write(234565, "test string")
        value1 = self.db.read(43556)
        value2 = self.db.read(234565)
        self.assertEqual(346735, value1)
        self.assertEqual("test string", value2)

    def test_string_insertions_restart(self):
        """Test 2 string insertions with restart"""
        self.db.write("test_str", "here is a test value")
        self.db.write("test_str2", "here is a test value2")
        self.restart()
        value1 = self.db.read("test_str")
        value2 = self.db.read("test_str2")
        self.assertEqual("here is a test value", value1)
        self.assertEqual("here is a test value2", value2)

    def test_int_insertions_restart(self):
        """test 2 int insertions with restart"""
        self.db.write("test_str", 346735)
        self.db.write("test_str2", 982745)
        self.restart()
        value1 = self.db.read("test_str")
        value2 = self.db.read("test_str2")
        self.assertEqual(346735, value1)
        self.assertEqual(982745, value2)

    def test_int_key_insertions_restart(self):
        """test 2 int key insertions with restart"""
        self.db.write(43556, 346735)
        self.db.write(234565, "test string")
        self.restart()
        value1 = self.db.read(43556)
        value2 = self.db.read(234565)
        self.assertEqual(346735, value1)
        self.assertEqual("test string", value2)

    def test_dict_insertion(self):
        """test inserting a dict"""
        self.db.write("test_str", {"key": "value", "sub": ["list", "of", "things"]})
        value = self.db.read("test_str")
        self.assertEqual({"key": "value", "sub": ["list", "of", "things"]}, value)

    def test_dict_insertion_restart(self):
        """test inserting a dict with restart"""
        self.db.write("test_str", {"key": "value", "sub": ["list", "of", "things"]})
        self.restart()
        value = self.db.read("test_str")
        self.assertEqual({"key": "value", "sub": ["list", "of", "things"]}, value)

    def test_deletion(self):
        """test deleting an element"""
        self.db.write("test_str", "here is a test value")
        self.db.write("test_str1", "here is a test value1")
        self.db.delete("test_str")
        with self.assertRaises(KeyError):
            self.db.read("test_str")

    def test_delete_read(self):
        """test deleting an element, and then reading another"""
        self.db.write("test_str", "here is a test value")
        self.db.write("test_str2", "here is a test value2")
        self.db.delete("test_str")
        self.assertEqual("here is a test value2", self.db.read("test_str2"))

    def test_delete_read_restart(self):
        """test deleting an element, and then reading another after a restart"""
        self.db.write("test_str", "here is a test value")
        self.db.write("test_str2", "here is a test value2")
        self.db.delete("test_str")
        self.restart()
        self.assertEqual("here is a test value2", self.db.read("test_str2"))
