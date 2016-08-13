import unittest
from unittest import TestCase
from BCAD_NoiseMit_Tools import WeaverUpdate as Tool


class TestWeaverUpdate(TestCase):
    def test_updateParameters(self):
        tool = Tool()
        params = tool.getParameterInfo()
        self.assertEquals(15, len(params))

    def test_execute(self):
        tool = Tool()
        params = tool.getParameterInfo()
        result = tool.execute(params, '#')
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()