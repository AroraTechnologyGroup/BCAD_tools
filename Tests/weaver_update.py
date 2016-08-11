from Tools.UpdateNoiseMitSDE import SdeConnector as Connector
from Tools.UpdateNoiseMitSDE import VersionManager as Manager
import arcpy
from arcpy import env
from arcpy import da
import traceback
import sys
import os
import logging

out_f = env.scratchFolder
# name of the connection file for the default version
out_n = "Weaver"

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


class FailedTest(Exception):
    """raise this exception when the test fails"""
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)


def test_connection():
    """out_folder, out_name, platform, instance, options"""
    connector = Connector(out_f, out_n, plat, inst, opt)
    output = connector.create_sde_connection()
    env.workspace = output
    try:
        datasets = arcpy.ListDatasets()
    except Exception as e:
        raise FailedTest(arcpy.GetMessages())
    if not output:
        raise FailedTest()
    else:
        os.remove(output)
        print "test_connection passed"
    return


def test_version_manager():
    """out_folder, platform, instance, target_sde, new_name, parent_version"""
    connector = Connector(out_f, out_n, plat, inst, opt)
    output = connector.create_sde_connection()
    manager = Manager(out_f, plat, inst, output, out_n, p_version)
    result = manager.clean_previous()
    version_sde = manager.connect_version()
    if not version_sde:
        raise FailedTest()
    result = manager.rec_post()
    if not result:
        raise FailedTest()
    manager.clean_previous()
    for x in [output, version_sde]:
        if os.path.exists(x):
            os.remove(x)


def run_tests():
    test_connection()
    test_version_manager()
