import os
import unittest
from unittest import TestCase

from BCAD_NoiseMit_Tools import WeaverGDBUpdate as PythonTool
from utils.UpdateNoiseMitSDE import VersionManager as Manager


class TestVersionManager(TestCase):
    @classmethod
    def setUpClass(cls):
        tool = PythonTool()
        parameters = tool.getParameterInfo()
        params = tool.process_parameters(parameters=parameters)
        cls.out_f = params["connection_folder"]
        cls.out_n = params["edit_connection_name"]
        cls.plat = params["platform"]
        cls.inst = params["instance"]
        cls.opt = params["opt"]
        cls.sde_file = params["gis_gdb"]
        cls.edit_version_name = params["edit_version_name"]
        cls.params = params

    def setUp(self):
        params = self.params
        out_f = self.out_f
        out_n = self.out_n
        plat = self.plat
        inst = self.inst
        edit_version_name = self.edit_version_name
        opt = self.opt

        self.manager = Manager(opt=opt, connection_folder=out_f, target_sde=self.sde_file,
                               new_version=edit_version_name, new_connection=out_n,
                               platform=plat, instance=inst)

    def tearDown(self):
        self.manager = None

    @classmethod
    def tearDownClass(cls):
        try:
            cls.params = None
            os.remove(cls.version_sde)
        except:
            pass

    def test_clean_previous(self):
        result = self.manager.clean_previous()
        self.assertTrue(result)

    def test_connect_version(self):
        out_f = self.out_f
        edit_version_name = self.edit_version_name
        self.version_sde = self.manager.connect_version()
        self.assertEqual("{}\\{}.sde".format(out_f, edit_version_name), self.version_sde)
        os.remove(self.version_sde)

    def test_rec_post(self):
        self.manager.clean_previous()
        self.manager.connect_version()
        result = self.manager.rec_post()
        self.assertTrue(result)


def suite():
    x = unittest.TestLoader().loadTestsFromTestCase(TestVersionManager)
    return unittest.TestSuite(x)

if __name__ == "__main__":
    unittest.main()
