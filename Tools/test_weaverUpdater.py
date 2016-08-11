import unittest
import arcpy
from arcpy import da
from unittest import TestCase
import UpdateNoiseMitSDE as Tool
from UpdateNoiseMitSDE import WeaverUpdater as Updater
from UpdateNoiseMitSDE import VersionManager as Manager
from UpdateNoiseMitSDE import SdeConnector as Connector
import os

# folder to store the connection files created in the script
out_f = r"C:\Users\rhughes\Documents\ArcGIS"
# name of the connection file to the default version
out_n = "Weaver.sde"

# name of the version to be created for editing
edit_version_name = "NoiseMit"

# name of the database instance
#inst = "ARORALAPTOP50\SDESQLEXPRESS"
inst = r"sql-server-azure.database.windows.net"

# username for the database user
#uid = "Richard"
uid = "bcad"

# password for the database user
#pwd = "Heddie01!"
pwd = "AroraGIS123"

# name of the database
#database = "DEVELOPMENT_BCAD.sde"
database = "bcad_noise"

# variable for the parent version of the database
p_version = "dbo.DEFAULT"

# name of building polygon feature class
bldgs = r"Building_Information"

# sql table used to update the geodatabase table
SQL_Table = ""

# GDB table with holds the weaver data from the sql table
GDB_Table = ""

# attribute fields on the building feature class that need to be selected by the user
building_attributes = {"Project Name": "projectName",
                       "Phase Name": "phaseName",
                       "Folio Number": "folioId"}

# attribute fields on the weaver geodatabase table that need to be selected by the user
weaver_attributes = {"Project Name": "ProjectName",
                     "Phase Name": "PhaseName",
                     "Folio Number": "FolioNumber"}

# name of the dataset that holds the Noise Mitigation data
dataset = "NoiseMitigation"

opt = {"account_authentication": "DATABASE_AUTH",
               "username": uid,
               "password": pwd,
               "save_user_pass": "SAVE_USERNAME",
               "database": database,
               "schema": "#",
               "version_type": "TRANSACTIONAL",
               "version": p_version,
               "date": ""}

plat = "SQL_SERVER"


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
        connector = Connector(out_folder=out_f, out_name=out_n, platform=plat, instance=inst, options=opt)
        cls.sde_file = connector.create_sde_connection()
        manager = Manager(out_folder=out_f, platform=plat, instance=inst, target_sde=cls.sde_file,
                          new_name="NoiseMit", parent_version=p_version)
        manager.clean_previous()
        cls.version_sde_file = manager.connect_version()
        SQL_Table = arcpy.ListTables("*weaver_formatted")[0]
        GDB_Table = arcpy.ListTables("*WEAVER")[0]
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