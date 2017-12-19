import os
import unittest
from unittest import TestCase

from BCAD_NoiseMit_Tools import WeaverGDBUpdate as PythonTool
from utils.UpdateNoiseMitSDE import SdeConnector


class TestSdeConnector(TestCase):
    @classmethod
    def setUpClass(cls):
        tool = PythonTool()
        params = tool.getParameterInfo()
        processed_params = tool.processParameters(params)
        cls.params = processed_params

    def setUp(self):
        params = self.params
        out_f = params["connection_folder"]
        out_n = params["edit_connection_name"]
        plat = params["platform"]
        inst = params["instance"]
        opt = params["opt"]

        self.connector = SdeConnector(out_f=out_f, out_name=out_n, platform=plat,
                                      instance=inst, options=opt)

    def tearDown(self):
        self.connector = None

    @classmethod
    def tearDownClass(cls):
        try:
            cls.params = None
        except:
            print("unable to remove the sde file created during the test")

    def test_create_sde_connection(self):
        params = self.params
        out_f = params["connection_folder"]
        out_n = params["edit_connection_name"]

        sde_file = self.connector.create_sde_connection()
        self.assertEqual("{}\\{}".format(out_f, out_n), sde_file)
        os.remove(sde_file)


def suite():
    x = unittest.TestLoader().loadTestsFromTestCase(TestSdeConnector)
    return unittest.TestSuite(x)

if __name__ == "__main__":
    unittest.main()
