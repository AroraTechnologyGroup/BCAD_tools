import unittest
from unittest import TestCase
from BCAD_NoiseMit_Tools import WeaverGDBUpdate, CARsGDBUpdate


class TestWeaverUpdate(TestCase):
    def setUp(self):
        self.tool = WeaverGDBUpdate()

    def tearDown(self):
        self.tool = None

    def test_processParameters(self):
        params = self.tool.getParameterInfo()
        final_params = self.tool.processParameters(params)
        self.assertEquals(17, len(final_params.keys()))

    def test_execute(self):
        params = self.tool.getParameterInfo()
        result = self.tool.execute(params, '#')
        self.assertTrue(result)


class TestSSACARUpdate(TestCase):
    def setUp(self):
        self.tool = CARsGDBUpdate()

    def tearDown(self):
        self.tool = None

    def test_processParameters(self):
        params = self.tool.getParameterInfo()
        final_params = self.tool.processParameters(params)
        self.assertEquals(16, len(final_params.keys()))

    def test_execute(self):
        params = self.tool.getParameterInfo()
        result = self.tool.execute(params, '#')
        self.assertTrue(result)


def suite():
    x = unittest.TestLoader().loadTestsFromTestCase(TestWeaverUpdate)
    y = unittest.TestLoader().loadTestsFromTestCase(TestSSACARUpdate)
    return unittest.TestSuite([x, y])


if __name__ == '__main__':
    unittest.main()
