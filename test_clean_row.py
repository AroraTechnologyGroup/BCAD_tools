import unittest
from unittest import TestCase
import UpdateNoiseMitSDE as Tool


class TestClean_row(TestCase):
    def test_clean_row(self):
        test_row = ["   ", None, "  apple", "tree  "]
        row = Tool.clean_row(test_row)
        print row
        self.assertListEqual(["", "unknown", "apple", "tree"], row)

if __name__ == '__main__':
    unittest.main()