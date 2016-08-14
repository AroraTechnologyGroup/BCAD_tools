from BCAD_NoiseMit_Tools import WeaverGDBUpdate as PythonTool
from UpdateNoiseMitSDE import SdeConnector
import unittest
from unittest import TestCase
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
            os.remove(cls.sde_file)
        except:
            print "unable to remove the sde file created during the test"

    def test_create_sde_connection(self):
        params = self.params
        out_f = params["out_f"]
        out_n = params["out_n"]
        self.sde_file = self.connector.create_sde_connection()
        self.assertEqual("{}\\{}".format(out_f, out_n), self.sde_file)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestSdeConnector)

if __name__ == "__main__":
    unittest.main()
