import os
import unittest
from unittest import TestCase

import arcpy
from arcpy import da, env

from BCAD_NoiseMit_Tools import WeaverGDBUpdate as PythonTool
from utils import UpdateNoiseMitSDE as Tool
from utils.UpdateNoiseMitSDE import GDBTableUpdater as Updater
from utils.UpdateNoiseMitSDE import VersionManager as Manager


class TestGDBTableUpdater(TestCase):

    @classmethod
    def setUpClass(cls):
        tool = PythonTool()
        parameters = tool.getParameterInfo()
        params = tool.processParameters(parameters=parameters)
        out_f = params["connection_folder"]
        out_n = params["edit_connection_name"]
        plat = params["platform"]
        inst = params["instance"]

        cls.weaver_attributes = params["table_attributes"]

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

        result = Tool.compare_tables(sql_table=sql_table, gdb_table=gdb_table)

        cls.match_fields = result["match_fields"]
        cls.add_rows = result["add_rows"]
        cls.exist_rows = result["exist_rows"]
        cls.folio_ids = result["folioIds"]

        env.workspace = cls.version_sde_file
        cls.editor = da.Editor(cls.version_sde_file)
        cls.version_gdb_table = arcpy.ListTables("*{}*".format(gdb_table_name))[0]

    def setUp(self):
        self.editor.startEditing()
        # All of the tests use this updater which has write rows and remove rows determined by the compare tables function
        self.updater = Updater(weaver_attributes=self.weaver_attributes, folioIds=self.folio_ids, match_fields=self.match_fields, write_table=self.version_gdb_table, read_rows=self.add_rows,
                               remove_rows=self.exist_rows, version_sde=self.version_sde_file, editor=self.editor)

    # def test_count_pid(self):
    #     result = self.updater.count_pid()
    #     if not len(self.updater.read_rows):
    #         self.assertEquals(result, {})
    #     else:
    #         self.assertTrue(result)

    def test_delete_rows(self):
        # change this to a folio in to be removed
        result = self.updater.delete_rows()
        if not len(self.updater.remove_rows):
            self.assertEquals(0, result)
        else:
            self.assertGreater(result, 0)

    def test_insert_rows(self):
        result = self.updater.insert_rows()
        if not len(self.updater.read_rows):
            self.assertEquals(0, result)
        else:
            self.assertEquals(len(self.updater.read_rows), result)

    def test_update_table(self):
        result = self.updater.update_table()
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
        self.editor.stopEditing(False)

    @classmethod
    def tearDownClass(cls):
        for x in [cls.version_sde_file]:
            try:
                os.remove(x)
            except:
                pass

        del cls.editor


def suite():
    x = unittest.TestLoader().loadTestsFromTestCase(TestGDBTableUpdater)
    return unittest.TestSuite(x)


if __name__ == "__main__":
    unittest.main()
