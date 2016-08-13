import sys
import os
from os.path import dirname, abspath
from UpdateNoiseMitSDE import SdeConnector
import unittest
from unittest import TestCase

out_f = r"C:\Users\rhughes\Documents\ArcGIS"
# name of the connection file for the default version
out_n = "Weaver.sde"

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


class TestSdeConnector(TestCase):

    def test_create_sde_connection(self):
        connector = SdeConnector(out_folder=out_f, out_name=out_n, platform=plat, instance=inst, options=opt)
        sde_file = connector.create_sde_connection()
        self.assertEqual("{}\\{}".format(out_f, out_n), sde_file)

if __name__ == '__main__':

    unittest.main()
