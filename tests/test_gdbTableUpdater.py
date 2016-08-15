import unittest
import arcpy
from arcpy import da, env
from unittest import TestCase
import UpdateNoiseMitSDE as Tool
from UpdateNoiseMitSDE import GDBTableUpdater as Updater
from UpdateNoiseMitSDE import VersionManager as Manager
from UpdateNoiseMitSDE import SdeConnector as Connector
import os
from BCAD_NoiseMit_Tools import WeaverGDBUpdate as PythonTool


class TestWeaverUpdater(TestCase):
    @classmethod
    def setUpClass(cls):
        tool = PythonTool()
        parameters = tool.getParameterInfo()
        params = tool.process_parameters(parameters=parameters)
        out_f = params["out_f"]
        out_n = params["out_n"]
        plat = params["plat"]
        inst = params["inst"]
        p_version = params["p_version"]
        edit_version_name = params["edit_version_name"]
        sql_table = params["sql_table"]
        gdb_table = params["gdb_table"]
        opt = params["opt"]
        uid = params["uid"]
        gdb_table_name = params["gdb_table_name"]

        connector = Connector(out_folder=out_f, out_name=out_n, platform=plat, instance=inst, options=opt)
        cls.sde_file = connector.create_sde_connection()
        manager = Manager(opt=opt, out_folder=out_f, uid=uid, platform=plat, instance=inst, target_sde=cls.sde_file,
                          new_name=edit_version_name, parent_version=p_version)
        manager.clean_previous()
        cls.version_sde_file = manager.connect_version()

        result = Tool.compare_fields(sql_table=sql_table, existing_table=gdb_table)

        cls.match_fields = result["match_fields"]
        cls.add_rows = result["add_rows"]
        cls.exist_rows = result["exist_rows"]

        env.workspace = cls.version_sde_file
        cls.editor = da.Editor(cls.version_sde_file)
        cls.editor.startEditing()

        cls.version_gdb_table = arcpy.ListTables("*{}".format(gdb_table_name))[0]

    def setUp(self):
        self.updater = Updater(match_fields=self.match_fields, write_table=self.version_gdb_table, read_rows=self.add_rows,
                               remove_rows=self.exist_rows, version_sde=self.version_sde_file, editor=self.editor)

    def test_count_pid(self):
        result = self.updater.count_pid()
        if not len(self.updater.read_rows):
            self.assertEquals(result, {})
        else:
            self.assertTrue(result)

    def test_insert_rows(self):
        result = self.updater.insert_rows()
        if not len(self.updater.read_rows):
            self.assertEquals(0, result)
        else:
            # change this value to the number of rows you expect to the inserted during the test
            self.assertEquals(44, result)

    def test_delete_rows(self):
        result = self.updater.delete_rows('4781')
        if not len(self.updater.remove_rows):
            self.assertFalse(result)
        else:
            # change this value to the number of rows expected to be deleted during the tests
            self.assertEquals(3, result)

    def test_update_table(self):
        result = self.updater.update_table('4781')
        if len(self.updater.read_rows) == len(self.updater.remove_rows):
            self.assertEqual(result[0], result[1])
        elif len(self.updater.read_rows) < len(self.updater.remove_rows):
            self.assertLess(result[0], result[1])
        elif len(self.updater.read_rows) > len(self.updater.remove_rows):
            self.assertGreater(result[0], result[1])

    def test_perform_update(self):
        result = self.updater.perform_update()
        self.assertTrue(result)

    def tearDown(self):
        self.updater = None

    @classmethod
    def tearDownClass(cls):
        for x in [cls.sde_file, cls.version_sde_file]:
            try:
                os.remove(x)
            except:
                pass

        cls.editor.stopEditing(False)
        del cls.editor


def suite():
    x = unittest.TestLoader().loadTestsFromTestCase(TestWeaverUpdater)
    return unittest.TestSuite(x)

if __name__ == "__main__":
    unittest.main()
