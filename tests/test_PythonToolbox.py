import unittest
from unittest import TestCase
from BCAD_NoiseMit_Tools import WeaverGDBUpdate as PythonTool


class TestWeaverUpdate(TestCase):
    def setUp(self):
        self.tool = PythonTool()

    def tearDown(self):
        self.tool = None

    def test_updateParameters(self):
        params = self.tool.getParameterInfo()
        self.assertEquals(15, len(params))

    def test_execute(self):
        params = self.tool.getParameterInfo()
        result = self.tool.execute(params, '#')
        self.assertTrue(result)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestWeaverUpdate)

if __name__ == '__main__':
    unittest.main()
