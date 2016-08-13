import os
import unittest
from unittest import TestCase

from UpdateNoiseMitSDE import SdeConnector as Connector
from UpdateNoiseMitSDE import VersionManager as Manager

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


class TestVersionManager(TestCase):
    def test_clean_previous(self):
        result = self.manager.clean_previous()
        self.assertTrue(result)

    def test_connect_version(self):
        version_sde = self.manager.connect_version()
        self.manager.version_sde = version_sde
        self.assertEqual("{}\\{}.sde".format(out_f, edit_version_name), version_sde)

    def test_rec_post(self):
        result = self.manager.rec_post()
        self.assertTrue(result)

    @classmethod
    def setUpClass(cls):
        connector = Connector(out_f, out_n, plat, inst, opt)
        cls.sde_file = connector.create_sde_connection()
        cls.manager = Manager(out_folder=out_f, platform=plat, instance=inst, target_sde=cls.sde_file,
                              new_name=edit_version_name, parent_version=p_version)

    @classmethod
    def tearDownClass(cls):
        if cls.sde_file:
            os.remove(cls.sde_file)
        if cls.manager.version_sde:
            os.remove(cls.manager.version_sde)


if __name__ == "__main__":
    unittest.main()
