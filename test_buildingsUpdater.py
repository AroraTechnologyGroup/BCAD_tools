import unittest
from unittest import TestCase
from BCAD_NoiseMit_Tools import WeaverGDBUpdate as PythonTool
from UpdateNoiseMitSDE import BuildingsUpdater as Updater
from UpdateNoiseMitSDE import SdeConnector as Connector
from UpdateNoiseMitSDE import VersionManager as Manager
from arcpy import da, env
import os


class TestBuildingsUpdater(TestCase):
    def test_get_folios(self):
        folios = self.updater.get_folios()
        self.assertTrue(folios)
        self.assertGreaterEqual(len(folios.keys()), 0)

    def test_update_buildings(self):
        result = self.updater.update_buildings()
        self.assertTrue(result)

    @classmethod
    def setUpClass(cls):
        tool = PythonTool()
        parameters = tool.getParameterInfo()
        params = tool.process_parameters(parameters=parameters)
        cls.params = params
        out_f = params["out_f"]
        out_n = params["out_n"]
        plat = params["plat"]
        inst = params["inst"]
        opt = params["opt"]
        uid = params["uid"]
        edit_version_name = params["edit_version_name"]
        p_version = params["p_version"]
        bldgs = params["bldgs"]
        GDB_Table = params["GDB_Table"]
        weav_atts = params["weaver_attributes"]
        bldg_atts = params["bldg_attributes"]

        #  out_folder, out_name, platform, instance, options
        cls.sde_file = Connector(out_folder=out_f, out_name=out_n, platform=plat,
                                 instance=inst, options=opt)
        # opt, out_folder, uid, platform, instance, target_sde, new_name, parent_version
        manager = Manager(opt=opt, out_folder=out_f, uid=uid, platform=plat, instance=inst,
                          target_sde=cls.sde_file, new_name=edit_version_name,
                          parent_version=p_version)
        cls.manager.clean_previous()
        cls.version_sde = manager.connect_version()
        edit = da.Editor(cls.version_sde)
        edit.startEditing()
        # bldgs, rel_table, bldg_atts, weav_atts, version_sde, editor
        cls.updater = Updater(bldgs=bldgs, rel_table=GDB_Table, bldg_atts=bldg_atts,
                              weav_atts=weav_atts, version_sde=cls.version_sde, editor=edit)

    @classmethod
    def tearDownClass(cls):
        cls.updater.editor.stopEditing(False)
        del cls.updater.editor
        env.workspace = cls.sde_file
        cls.manager.clean_previous()
        for x in [cls.version_sde, cls.sde_file]:
            try:
                os.remove(x)
            except:
                pass

if __name__ == '__main__':
    unittest.main()