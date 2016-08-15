import unittest
from unittest import TestCase
from BCAD_NoiseMit_Tools import WeaverGDBUpdate as PythonTool
from UpdateNoiseMitSDE import BuildingsUpdater as Updater
from UpdateNoiseMitSDE import SdeConnector as Connector
from UpdateNoiseMitSDE import VersionManager as Manager
from arcpy import da, env
import os


class TestBuildingsUpdater(TestCase):
    @classmethod
    def setUpClass(cls):
        tool = PythonTool()
        parameters = tool.getParameterInfo()
        params = tool.process_parameters(parameters=parameters)
        cls.params = params
        buildings_name = params["buildings_name"]
        out_f = params["out_f"]
        out_n = params["out_n"]
        plat = params["plat"]
        inst = params["inst"]
        opt = params["opt"]
        uid = params["uid"]
        edit_version_name = params["edit_version_name"]
        p_version = params["p_version"]

        #  out_folder, out_name, platform, instance, options
        connector = Connector(out_folder=out_f, out_name=out_n, platform=plat,
                              instance=inst, options=opt)
        cls.sde_file = connector.create_sde_connection()
        # opt, out_folder, uid, platform, instance, target_sde, new_name, parent_version
        cls.manager = Manager(opt=opt, out_folder=out_f, uid=uid, platform=plat, instance=inst,
                              target_sde=cls.sde_file, new_name=edit_version_name,
                              parent_version=p_version)
        cls.manager.clean_previous()
        cls.version_sde = cls.manager.connect_version()
        env.workspace = cls.version_sde
        cls.versioned_buildings = tool.get_versioned_fc(env.workspace, buildings_name)
        cls.edit = da.Editor(cls.version_sde)
        cls.edit.startEditing()

    def setUp(self):

        GDB_Table = self.params["gdb_table"]
        weav_atts = self.params["weaver_attributes"]
        bldg_atts = self.params["building_attributes"]
        bldgs = self.versioned_buildings
        # bldgs, rel_table, bldg_atts, weav_atts, version_sde, editor
        self.updater = Updater(bldgs=bldgs, rel_table=GDB_Table, bldg_atts=bldg_atts,
                               weav_atts=weav_atts, version_sde=self.version_sde, editor=self.edit)

    def tearDown(self):
        self.updater = None

    @classmethod
    def tearDownClass(cls):
        cls.edit.stopEditing(False)
        del cls.edit
        env.workspace = cls.sde_file
        cls.manager.clean_previous()
        for x in [cls.version_sde, cls.sde_file]:
            try:
                os.remove(x)
            except:
                pass

    def test_get_folios(self):
        folios = self.updater.get_folios()
        self.assertTrue(folios)
        self.assertGreaterEqual(len(folios.keys()), 0)

    def test_update_buildings(self):
        result = self.updater.update_buildings()
        self.assertTrue(result)


def suite():
    x = unittest.TestLoader().loadTestsFromTestCase(TestBuildingsUpdater)
    return unittest.TestSuite(x)

if __name__ == '__main__':
    unittest.main()
