import arcpy
import os
import sys
import logging
from arcpy import da
from arcpy import env
import pyodbc
from collections import Counter
import traceback

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


def count_pid(in_table):
    parcel_ids = []
    read_cursor = da.SearchCursor(in_table, 'ParcelID')
    for _row in read_cursor:
        parcel_ids.append(_row[0])
    del read_cursor

    _cnt = Counter()
    for _pid in parcel_ids:
        _cnt[_pid] += 1

    items = dict(_cnt)
    return items


def update_table(parcel_id, n_rows, remove_rows, in_table, fields):
    try:
        # remove read rows that exist in the target table
        def compare_cells(_replace, _index, t_line, comp_rows):
            comp_row = comp_rows[_index]
            if _index:
                # compare row to previous rows if index != 0
                if t_line in comp_rows:
                    # exit the function if the row already exists in the reader rows
                    # all filtering should be done before entering the cursor so this should not
                    # ever return at this point
                    return 0, 0

            attr = list(t_line)
            comp = list(comp_row)
            zipped = zip(attr, comp)
            new_row = []

            for tup in zipped:
                new_row.append(tup[1])
                if tup[0] != tup[1]:
                    _replace += 1

            return _replace, new_row

        def swap_rows(input_rows, _rem_rows):
            for _row in input_rows:
                index = input_rows.index(_row)
                replace = 0
                edit.startOperation()
                with da.UpdateCursor(in_table, fields, "ParcelID='{}'".format(parcel_id)) as cursor:
                    for line in cursor:
                        # update all rows from the remove_rows list
                        if line in _rem_rows:
                            replace, n_row = compare_cells(replace, index, line, n_rows)
                            if replace:
                                cursor.updateRow(n_row)
                                break
                edit.stopOperation()

        def delete_rows(_rem_rows):
            edit.startOperation()
            with da.UpdateCursor(in_table, fields, "ParcelID='{}'".format(parcel_id)) as cursor:
                for line in cursor:
                    # delete all rows from the input list
                    if line in _rem_rows:
                        cursor.deleteRow(line)

        # use the update cursor to update the rem_rows with the in_rows values
        if len(n_rows) == len(remove_rows):
            swap_rows(n_rows, remove_rows)
            pass

        elif len(n_rows) > len(remove_rows):
            sl1 = n_rows[:len(remove_rows)]
            sl2 = n_rows[len(remove_rows):]

            swap_rows(sl1, remove_rows)

            insert_rows(sl2, in_table)

        elif len(n_rows) < len(remove_rows):
            sl1 = remove_rows[:len(n_rows)]
            sl2 = remove_rows[len(n_rows):]

            swap_rows(n_rows, sl1)

            delete_rows(sl2)

    except Exception as f:
        print f.message

    return


def insert_rows(input_rows, table, fields):
    try:
        edit.startOperation()

        insert = da.InsertCursor(table, fields)

        for _row in input_rows:
            insert.insertRow(_row)
        del insert

        edit.stopOperation()

    except Exception as h:
        print h.message


def create_memory_objects():
    # create variables for table and building feature class memory objects
    memory_table = "in_memory\\noise_mit"
    memory_buildings = "in_memory\\buildings"

    # Delete the in memory objects if they were not cleaned during previous run
    if arcpy.Exists(memory_table):
        arcpy.Delete_management(memory_table)
    if arcpy.Exists(memory_buildings):
        arcpy.Delete_management(memory_buildings)
    return memory_table, memory_buildings

# connection info for the odbc connection
driver = 'SQL Server Native Client 11.0'
server = r'ARORALAPTOP50\SDESQLEXPRESS'
port = 1433
database = 'DEVELOPMENT_BCAD'
uid = 'Richard'
pwd = 'AroraGIS123'

# connection info for the esri sde connection
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

    # verify that the necessary tables exist
    for x in [read_table, write_table, buildings]:
        if not arcpy.Exists(x):
            logging.error("the target table and the building are not found")
            raise Exception()

    # build new version
    versions = da.ListVersions(targetSDE)

    if len(versions):
        v_names = [v.name for v in versions]
        if version_name in v_names:
            try:
                arcpy.DeleteVersion_management(targetSDE, version_name)
            except:
                logging.info(arcpy.GetMessages())
    else:
        logging.info("no versions found")

    arcpy.CreateVersion_management(targetSDE, pversion, new_name)

    # create an sde connection file to the new version
    out_name = "{}.sde".format(uid)
    options["version"] = version_name
    sde_version = create_sde_connection(out_folder, out_name, platform, instance, options)
    # input path to the dev sde database connection
    version_SDE = sde_version.getOutput(0)
    if not os.path.exists(version_SDE):
        raise Exception("The sde file was not created")

    # connect using the new sde_version file
    env.workspace = version_SDE
    # get the number for rows per parcel ID
    pid_list = count_pid(read_table)
    # begin edit session
    edit = da.Editor(version_SDE)
    edit.startEditing()
    # work with one pid at a time
    for pid in pid_list.keys():
        # number of rows that should be exist in the target table for the pid
        cnt = pid_list[pid]
        # pull the rows into a list

        def get_rows(in_table, _pid):
            reader = da.SearchCursor(in_table, "*", "ParcelID='{}'".format(_pid))
            # rows contains all of the rows with the pid
            rows = []
            for row in reader:
                rows.append(row)
            del reader
            return rows

        read_rows = get_rows(read_table, pid)
        exist_rows = get_rows(write_table, pid)
        read_rows.sort()
        exist_rows.sort()

        in_rows = [r for r in read_rows if r not in exist_rows]
        rem_rows = [r for r in exist_rows if r not in read_rows]

        t_fields = arcpy.ListFields(write_table)
        t_fields = [t.name for t in t_fields]
        t_fields.remove('OBJECTID')

        # table is empty, populate it
        if len(exist_rows) == 0:
            edit.startOperation()
            insert_rows(read_rows, write_table, t_fields)
            edit.stopOperation()
            pass

        else:
            update_table(pid, in_rows, rem_rows, write_table, t_fields)

    edit.stopEditing(True)

    # arcpy.ChangeVersion_management(write_table, "TRANSACTIONAL", version_name)

    # building_layer = arcpy.MakeFeatureLayer_management(buildings, memory_buildings)
    # arcpy.ChangeVersion_management(building_layer, "TRANSACTIONAL", version_name)

except Exception as e:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)




