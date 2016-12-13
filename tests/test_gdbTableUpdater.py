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
        out_f = params["connection_folder"]
        out_n = params["edit_connection_name"]
        plat = params["platform"]
        inst = params["instance"]

        edit_version_name = params["edit_version_name"]
        sql_table = params["sql_table"]
        gdb_table = params["gdb_table"]
        gdb_table_name = params["gdb_table_name"]
        opt = params["opt"]

        cls.sde_file = params["gis_gdb"]
        manager = Manager(opt=opt, connection_folder=out_f, target_sde=cls.sde_file,
                          new_version=edit_version_name, new_connection=out_n, platform=plat, instance=inst)
        manager.clean_previous()
        cls.version_sde_file = manager.connect_version()

        result = Tool.compare_fields(sql_table=sql_table, gdb_table=gdb_table)

        cls.match_fields = result["match_fields"]
        cls.add_rows = result["add_rows"]
        cls.exist_rows = result["exist_rows"]

        env.workspace = cls.version_sde_file
        cls.editor = da.Editor(cls.version_sde_file)
        cls.editor.startEditing()

        cls.version_gdb_table = arcpy.ListTables("*{}*".format(gdb_table_name))[0]

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
            self.assertEquals(len(self.updater.read_rows), result)

    def test_delete_rows(self):
        result = self.updater.delete_rows('4781')
        if not len(self.updater.remove_rows):
            self.assertFalse(result)
        else:
            # change this value to the number of rows expected to be deleted during the tests
            self.assertGreater(result, 0)

    def test_update_table(self):
        result = self.updater.update_table('4781')
        if len(self.updater.read_rows) == len(self.updater.remove_rows):
            self.assertEqual(result[0], result[1])
        elif len(self.updater.read_rows) < len(self.updater.remove_rows):
            self.assertGreater(result[0], result[1])
        elif len(self.updater.read_rows) > len(self.updater.remove_rows):
            self.assertLess(result[0], result[1])

    def test_perform_update(self):
        result = self.updater.perform_update()
        self.assertTrue(result)

    def tearDown(self):
        self.updater = None

    @classmethod
    def tearDownClass(cls):
        for x in [cls.version_sde_file]:
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
