import unittest
from unittest import TestCase
import UpdateNoiseMitSDE as Tool


class TestRun_tool(TestCase):
    """This test runs the tool with the variables specified in the tool"""
    def test_run_tool(self):
        x = Tool.run_tool()
        self.assertTrue(x)

if __name__ == "__main__":
    unittest.main()


