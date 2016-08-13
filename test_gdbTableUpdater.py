import unittest
from arcpy import da
from unittest import TestCase
import UpdateNoiseMitSDE as Tool
from UpdateNoiseMitSDE import GDBTableUpdater as Updater
from UpdateNoiseMitSDE import VersionManager as Manager
from UpdateNoiseMitSDE import SdeConnector as Connector
import os
from BCAD_NoiseMit_Tools import WeaverUpdate as PythonTool


class TestWeaverUpdater(TestCase):
    def test_count_pid(self):
        result = self.updater.count_pid()
        self.assertTrue(result)

    def test_insert_rows(self):
        result = self.updater.insert_rows()
        self.assertEquals(44, result)

    def test_delete_rows(self):
        result = self.updater.delete_rows('4781')
        self.assertFalse(result)

    def test_update_table(self):
        result = self.updater.update_table('4781')
        self.assertLess(result[0], result[1])

    def test_perform_update(self):
        result = self.updater.perform_update()
        self.assertTrue(result)

    @classmethod
    def setUpClass(cls):
        params = PythonTool.getParameterInfo()
        out_f, inst, uid, pwd, database, p_version, bldgs, \
        SQL_Table, GDB_Table, bldg_projectName, bldg_phaseName, \
        bldg_folioId, gdb_table_projectName, gdb_table_phaseName = [p.valueAsText for p in params[:-1]]

        gdb_table_folioId, GDB_Table_name, Buildings_name, out_n, edit_connection_name, \
        opt, plat, building_attributes, weaver_attributes = [p for p in params[-1]]

        connector = Connector(out_folder=out_f, out_name=out_n, platform=plat, instance=inst, options=opt)
        cls.sde_file = connector.create_sde_connection()
        manager = Manager(out_folder=out_f, platform=plat, instance=inst, target_sde=cls.sde_file,
                          new_name="NoiseMit", parent_version=p_version)
        manager.clean_previous()
        cls.version_sde_file = manager.connect_version()
        result = Tool.compare_fields(sql_table=SQL_Table, existing_table=GDB_Table)
        match_fields = result["match_fields"]
        add_rows = result["add_rows"]
        exist_rows = result["exist_rows"]
        editor = da.Editor(cls.version_sde_file)
        editor.startEditing()
        cls.updater = Updater(match_fields=match_fields, write_table=GDB_Table, read_rows=add_rows,
                              remove_rows=exist_rows, version_sde=cls.version_sde_file, editor=editor)

    @classmethod
    def tearDownClass(cls):
        for x in [cls.sde_file, cls.version_sde_file]:
            os.remove(x)
        cls.updater.editor.stopEditing(False)
        del cls.updater.editor

if __name__ == "__main__":
    unittest.main()
