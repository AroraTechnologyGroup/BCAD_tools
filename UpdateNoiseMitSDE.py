import arcpy
import os
import sys
import logging
from arcpy import da
from arcpy import env
import pyodbc

log_name = "C:\\Temp\NoiseMit_logfile.txt"

logging.basicConfig(level=logging.DEBUG,
                    filename=log_name,
                    filemode='w')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def create_sql_connection(d, s, p, db, u, pw):
    connection = pyodbc.connect(r"DRIVER={0};SERVER={1};PORT={2};DATABASE={3};UID={4};PWD={5}".format(d, s, p,
                                                                                                      db, u, pw))
    cr = connection.cursor()
    d = {"connection": connection, "cursor": cr}
    return d


def create_sde_connection(out_folder_path, name, database_platform, inst, kwargs):
    # delete the sde file if it exists
    loc = os.path.join(out_folder, out_name)
    if os.path.exists(loc):
        os.remove(loc)

    sd = arcpy.CreateDatabaseConnection_management(out_folder_path, name, database_platform, inst, **kwargs)
    return sd


def compare_fields(read_wc, write_wc):
    """Compare the fields between the tables to catch a schema change"""
    r_table = ""
    tables = arcpy.ListTables(read_wc)
    if len(tables):
        r_table = tables[0]

    r_fields = arcpy.ListFields(r_table)
    r_fnames = [f.name for f in r_fields]

    w_table = ""
    tables = arcpy.ListTables(write_wc)
    if len(tables):
        w_table = tables[0]
    w_fields = arcpy.ListFields(w_table)
    w_fnames = [f.name for f in w_fields]

    # The only missing field should be ObjectID
    # TODO-New fields should prompt an Exception due to schema change
    new_fields = [f for f in r_fnames if f not in w_fnames]
    missing_fields = [f for f in w_fnames if f not in r_fnames]

    if len(new_fields):
        raise Exception("A schema change exists in the updated weaver table :: {}".format(new_fields))
    if missing_fields != [u'OBJECTID']:
        raise Exception("The updated weaver table is missing fields :: {}".format(missing_fields))

    return [r_table, w_table]

driver = 'SQL Server Native Client 11.0'
server = r'ARORALAPTOP50\SDESQLEXPRESS'
port = 1433
database = 'DEVELOPMENT_BCAD'
uid = 'Richard'
pwd = 'AroraGIS123'

# Create Esri Database Connection
out_folder = env.scratchFolder
out_name = "Weaver.sde"
platform = "SQL_SERVER"
instance = server

options = {"account_authentication": "DATABASE_AUTH",
           "username": uid,
           "password": pwd,
           "save_user_pass": "SAVE_USERNAME",
           "database": database,
           "schema": "#",
           "version_type": "TRANSACTIONAL",
           "version": "dbo.Default",
           "date": ""}
try:
    #cnxn = create_sql_connection(driver, server, port, database, uid, pwd)
    sde = create_sde_connection(out_folder, out_name, platform, instance, options)

    # input path to the dev sde database connection
    targetSDE = sde.getOutput(0)
    if not os.path.exists(targetSDE):
        raise Exception("The sde file was not created")

    env.workspace = targetSDE

    # create a new version to put edits into
    pversion = "dbo.DEFAULT"
    version_name = "{}.NoiseMit".format(uid.upper())
    new_name = "NoiseMit"
    write_table = r""
    buildings = r"DEVELOPMENT_BCAD.DBO.NoiseMitigation\\DEVELOPMENT_BCAD.DBO.Building_Information"

    read_table, write_table = compare_fields("*weaver_formatted", "*WEAVER")
    versions = da.ListVersions(targetSDE)
    if len(versions):
        vnames = [v.name for v in versions]
        if version_name in vnames:
            try:
                arcpy.DeleteVersion_management(targetSDE, version_name)
            except:
                logging.info(arcpy.GetMessages())
    else:
        logging.info("no versions found")

    arcpy.CreateVersion_management(targetSDE, pversion, new_name)
    arcpy.ChangeVersion_management(write_table, "TRANSACTIONAL", version_name)

    # obtain table and building feature class objects

    table_view = ""
    building_layer = ""
    memory_table = "in_memory\\noise_mit"
    memory_buildings = "in_memory\\buildings"
    for x in [write_table, buildings]:
        if not arcpy.Exists(x):
            logging.error("the target table and the building are not found")
            raise Exception()

    # Delete the in memory objects
    if arcpy.Exists(memory_table):
        arcpy.Delete_management(memory_table)
    if arcpy.Exists(memory_buildings):
        arcpy.Delete_management(memory_buildings)

    table_view = arcpy.MakeTableView_management(write_table, memory_table)
    building_layer = arcpy.MakeFeatureLayer_management(buildings, memory_buildings)

    read_cursor = da.SearchCursor(read_table, ['*'])

    with da.UpdateCursor(table_view, ["*"]) as cursor:
        for row in cursor:
            r_value = read_cursor.next()
            if row[1:] == r_value:
                print row
            else:
                print "No match between {} and {}".format(r_value, row[1:])
except Exception as e:
    print e.message, arcpy.GetMessages()




