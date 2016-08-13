from BCAD_NoiseMit_Tools import WeaverGDBUpdate as PythonTool
from UpdateNoiseMitSDE import SdeConnector
import unittest
from unittest import TestCase


class TestSdeConnector(TestCase):

    def test_create_sde_connection(self):
        params = self.params
        out_f = params["out_f"]
        out_n = params["out_n"]
        plat = params["plat"]
        inst = params["inst"]
        opt = params["opt"]

        connector = SdeConnector(out_folder=out_f, out_name=out_n, platform=plat,
                                 instance=inst, options=opt)
        sde_file = connector.create_sde_connection()
        self.assertEqual("{}\\{}".format(out_f, out_n), sde_file)

    @classmethod
    def setUpClass(cls):
        tool = PythonTool()
        params = tool.getParameterInfo()
        processed_params = tool.process_parameters(params)
        cls.params = processed_params

if __name__ == '__main__':
    unittest.main()
