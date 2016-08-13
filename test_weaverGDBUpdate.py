import unittest
from unittest import TestCase
from BCAD_NoiseMit_Tools import WeaverGDBUpdate as PythonTool


class TestWeaverUpdate(TestCase):
    def test_updateParameters(self):
        params = self.tool.getParameterInfo()
        self.assertEquals(16, len(params))

    def test_execute(self):
        params = self.tool.getParameterInfo()
        result = self.tool.execute(params, '#')
        self.assertTrue(result)

    @classmethod
    def setUpClass(cls):
        cls.tool = PythonTool()

if __name__ == '__main__':
    unittest.main()