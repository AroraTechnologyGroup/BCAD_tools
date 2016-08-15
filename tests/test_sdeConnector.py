from BCAD_NoiseMit_Tools import WeaverGDBUpdate as PythonTool
from UpdateNoiseMitSDE import SdeConnector
import unittest
from unittest import TestCase
import gc
import os


class TestSdeConnector(TestCase):
    @classmethod
    def setUpClass(cls):
        tool = PythonTool()
        params = tool.getParameterInfo()
        processed_params = tool.process_parameters(params)
        cls.params = processed_params

    def setUp(self):
        params = self.params
        out_f = params["out_f"]
        out_n = params["out_n"]
        plat = params["plat"]
        inst = params["inst"]
        opt = params["opt"]

        self.connector = SdeConnector(out_folder=out_f, out_name=out_n, platform=plat,
                                      instance=inst, options=opt)

    def tearDown(self):
        self.connector = None

    @classmethod
    def tearDownClass(cls):
        try:
            cls.params = None
        except:
            print "unable to remove the sde file created during the test"

    def test_create_sde_connection(self):
        params = self.params
        out_f = params["out_f"]
        out_n = params["out_n"]
        sde_file = self.connector.create_sde_connection()
        self.assertEqual("{}\\{}".format(out_f, out_n), sde_file)
        os.remove(sde_file)


def suite():
    x = unittest.TestLoader().loadTestsFromTestCase(TestSdeConnector)
    return unittest.TestSuite(x)

if __name__ == "__main__":
    unittest.main()
