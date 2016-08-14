import os
import unittest
from unittest import TestCase
from BCAD_NoiseMit_Tools import WeaverGDBUpdate as PythonTool
from UpdateNoiseMitSDE import SdeConnector as Connector
from UpdateNoiseMitSDE import VersionManager as Manager


class TestVersionManager(TestCase):
    @classmethod
    def setUpClass(cls):
        tool = PythonTool()
        parameters = tool.getParameterInfo()
        params = tool.process_parameters(parameters=parameters)
        out_f = params["out_f"]
        out_n = params["out_n"]
        plat = params["plat"]
        inst = params["inst"]
        opt = params["opt"]

        cls.params = params
        connector = Connector(out_f, out_n, plat, inst, opt)
        cls.sde_file = connector.create_sde_connection()

    def setUp(self):
        params = self.params
        out_f = params["out_f"]
        plat = params["plat"]
        inst = params["inst"]
        p_version = params["p_version"]
        edit_version_name = params["edit_version_name"]
        self.manager = Manager(out_folder=out_f, platform=plat, instance=inst,
                               target_sde=self.sde_file, new_name=edit_version_name,
                               parent_version=p_version)

    def tearDown(self):
        self.manager = None

    @classmethod
    def tearDownClass(cls):
        for x in [cls.version_sde, cls.sde_file]:
            try:
                os.remove(x)
            except:
                pass

    def test_clean_previous(self):
        result = self.manager.clean_previous()
        self.assertTrue(result)

    def test_connect_version(self):
        out_f = self.params["out_f"]
        edit_version_name = self.params["edit_version_name"]
        self.version_sde = self.manager.connect_version()
        self.assertEqual("{}\\{}.sde".format(out_f, edit_version_name), self.version_sde)

    def test_rec_post(self):
        result = self.manager.rec_post()
        self.assertTrue(result)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestVersionManager)

if __name__ == "__main__":
    unittest.main()
