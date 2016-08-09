import arcpy
import os
import sys
import logging
from arcpy import da
from arcpy import env
import pyodbc
from collections import Counter
import traceback
env.overwriteOutput = 1
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


# connection info for the odbc connection
driver = 'SQL Server Native Client 11.0'
server = r'ARORALAPTOP50\SDESQLEXPRESS'
port = 1433
database = 'DEVELOPMENT_BCAD'
uid = 'Richard'
pwd = 'AroraGIS123'

# connection info for the esri sde connection
out_f = env.scratchFolder
out_n = "Weaver.sde"
plat = "SQL_SERVER"
inst = server

opt = {"account_authentication": "DATABASE_AUTH",
           "username": uid,
           "password": pwd,
           "save_user_pass": "SAVE_USERNAME",
           "database": database,
           "schema": "#",
           "version_type": "TRANSACTIONAL",
           "version": "dbo.DEFAULT",
           "date": ""}

# create a new version to put edits into
p_version = "dbo.DEFAULT"
v_name = "{}.NoiseMit".format(uid.upper())
n_name = "NoiseMit"
bldgs = r"Building_Information"
update_atts = ["PROJECTNAM", "PHASENAME"]
folio_num_field = "FOLIONUMBE"
dataset = "NoiseMitigation"


class VersionException(Exception):
    """throw this when handling exception with managing versions"""
    def __init__(self, *args):
        exc_t, exc_v, exc_trace = sys.exc_info()
        traceback.print_exception(exc_t, exc_v, exc_trace,
                                  limit=2, file=sys.stdout)


class SdeConnector:
    def __init__(self, out_folder, out_name, platform, instance, options):
        self.out_folder = out_folder
        self.out_name = out_name
        self.platform = platform
        self.instance = instance
        self.options = options

    def create_sde_connection(self):
        # delete the sde file if it exists
        loc = os.path.join(self.out_folder, self.out_name)
        if os.path.exists(loc):
            os.remove(loc)

        sd = arcpy.CreateDatabaseConnection_management(self.out_folder, self.out_name, self.platform, self.instance,
                                                       **self.options)
        # input path to the dev sde database connection
        target_sde = sd.getOutput(0)
        if not os.path.exists(target_sde):
            raise Exception("The sde file was not created")

        env.workspace = target_sde

        return target_sde


class VersionManager:
    def __init__(self, out_folder, platform, instance, target_sde, version_name, new_name, parent_version, log):
        self.instance = instance
        self.platform = platform
        self.out_folder = out_folder
        self.target_sde = target_sde
        self.version_name = version_name
        self.new_name = new_name
        self.version_sde = ""
        self.parent_version = parent_version
        self.log = log
        pass

    def clean_previous(self):
        # remove previous version if it exists
        versions = da.ListVersions(self.target_sde)
        if len(versions):
            v_names = [v.name for v in versions]
            if self.version_name in v_names:
                try:
                    arcpy.DeleteVersion_management(self.target_sde, self.version_name)
                except:
                    raise VersionException()
        else:
            logging.info("no versions found")

    def connect_version(self):
        # create version to edit
        versions = da.ListVersions(self.target_sde)
        if len(versions):
            v_names = [v.name for v in versions]
            if self.version_name in v_names:
                raise VersionException("Version already exists, must remove before proceeding")
            else:
                try:
                    arcpy.CreateVersion_management(self.target_sde, self.parent_version, self.new_name)
                except:
                    raise VersionException()


            # create an sde connection file to the new version
            v_opt = opt.copy()
            v_opt["version"] = self.version_name

            # create SdeConnector object for the version
            version_connection = SdeConnector(self.out_folder, self.new_name, self.platform, self.instance, v_opt)
            self.version_sde = version_connection.create_sde_connection()
            return self.version_sde

        else:
            raise VersionException()

    def rec_post(self):
        workspace = self.version_sde
        env.workspace = workspace
        # You should be on the only user
        userList = arcpy.ListUsers(workspace)
        arcpy.AcceptConnections(workspace, False)
        edit = da.Edit(self.workspace)
        edit.startEditing()
        edit.startOperation()
        arcpy.ReconcileVersions_management(workspace, "ALL_VERSIONS", self.parent_version,
                                           self.version_name, "LOCK_ACQUIRED",
                                           "ABORT_CONFLICTS", "BY_OBJECT", "FAVOR_TARGET_VERSION",
                                           "POST", "KEEP_VERSION")
        edit.stopOperation()
        edit.stopEditing(True)
        env.workspace = self.target_sde
        self.clean_previous()
        return True


class WeaverUpdater:
    #w_table, add_rows, rem_rows, version_sde_file
    def __init__(self, write_table, read_rows, remove_rows, version_sde):
        self.write_table = write_table
        self.read_rows = read_rows
        self.remove_rows = remove_rows
        self.version_sde = version_sde
        self.items = {}
        self.all_fields = []
        pass

    def count_pid(self):
        parcel_ids = []

        for _row in self.read_rows:
            parcel_ids.append(_row[0])

        _cnt = Counter()
        for _pid in parcel_ids:
            _cnt[_pid] += 1

        self.items = dict(_cnt)
        return self.items

    def insert_rows(self, editor):
        try:
            editor.startOperation()

            insert = da.InsertCursor(self.write_table, self.all_fields)

            for _row in self.read_rows:
                insert.insertRow(_row)
            del insert

            editor.stopOperation()

        except Exception as h:
            print h.message

    def delete_rows(self, parcel_id, editor):
        editor.startOperation()
        with da.UpdateCursor(self.write_table, self.all_fields, "ParcelID='{}'".format(parcel_id)) as _cursor:
            for line in _cursor:
                # delete all rows from the input list
                if line in self.remove_rows:
                    _cursor.deleteRow(line)
        editor.startOperation()

    def update_table(self, parcel_id, editor):
        try:
            # use the update cursor to remove the rem_rows
            if len(self.remove_rows):
                self.delete_rows(parcel_id, editor)
            elif len(self.read_rows):
                self.insert_rows(editor)

        except Exception as f:
            print f.message

        return

    def perform_update(self):
        # begin edit session
        edit = da.Editor(self.version_sde)
        edit.startEditing()
        # work with one pid at a time
        for pid in self.items.keys():

            t_fields = arcpy.ListFields(self.write_table)
            t_fields = [t.name for t in t_fields]
            t_fields.remove('OBJECTID')
            self.all_fields = t_fields

            # table is empty, populate it
            if len(self.remove_rows) == 0:
                self.insert_rows(edit)
                pass
            # update the rows that are not identical, a complex function
            else:
                self.update_table(pid, edit)

        edit.stopEditing(True)
        return True


class BuildingsUpdater:
    """buildings, w_table, folio_num_field, update_atts, version_sde_file"""
    def __init__(self, b, rel_table, folio_field, update_fields, version_sde):
        self.buildings = b
        self.version_sde = version_sde
        self.folio_field = folio_field
        self.update_fields = update_fields
        self.rel_table = rel_table
        self.folios = []

    def get_fields(self, in_table):
        ups = []
        # list fields to get the correct field name if capitalization is not identical
        fields = arcpy.ListFields(in_table)
        for f in fields:
            if f.name.upper() in self.update_fields:
                ups.append(f.name)
        return ups

    def get_folios(self):
        with da.SearchCursor(self.buildings, [self.folio_field]) as _cursor:
            for _row in _cursor:
                self.folios.append(_row[0])

        return self.folios

    def update_buildings(self):
        # gather the building folio numbers
        _folios = self.get_folios()
        edit = da.Editor(self.version_sde)
        edit.startEditing()
        for folio in _folios:
            matrix = []

            fields = self.get_fields(self.rel_table)
            with da.SearchCursor(self.rel_table, fields, "FolioNumber='{}'".format(self.folio_field.capitalize())) as _cursor:
                for _row in _cursor:
                    matrix.append(_row)
            comps = list(zip(*matrix))

            fields = self.get_fields(self.buildings)
            edit.startOperation()
            with da.UpdateCursor(self.buildings, fields, "{}='{}'".format(self.folio_field.upper(), folio)) as _cursor:
                for _row in _cursor:
                    new_row = []
                    for _x in comps:
                        s = set(_x)
                        if len(s) > 1:
                            new_row.append(", ".join(s))
                        elif len(s) == 1:
                            new_row.append(s.pop())
                    _cursor.updateRow(new_row)
            edit.stopOperation()
        edit.stopEditing(True)
        return True

try:
    #cnxn = create_sql_connection(driver, server, port, database, uid, pwd)
    connection = SdeConnector(out_f, out_n, plat, inst, opt)
    sde_file = connection.create_sde_connection()

    r_table, w_table = compare_fields("*weaver_formatted", "*WEAVER")

    # verify that the necessary tables exist
    for x in [r_table, w_table]:
        if not arcpy.Exists(x):
            logging.error("the target table and the building are not found")
            raise Exception()

    # Add all of the rows from the weaver sql table to a list
    add_rows = []
    with da.SearchCursor(r_table, "*") as cursor:
        for row in cursor:
            add_rows.append(row)
    del cursor

    exist_rows = []
    with da.SearchCursor(w_table, "*") as cursor:
        for row in cursor:
            # the rows from the GDB table, are identical to any row in the list, remove that row from the list
            if row[1:] in add_rows:
                add_rows.remove(row[1:])
                pass
            # if the row is not in the add_rows, then add it to the exist_rows to remove
            else:
                exist_rows.append(row)
    del cursor

    compare_result = 0
    if len(add_rows) or len(exist_rows):
        compare_result += 1

    if compare_result:
        # create VersionManager class object to create new version, connect to it,
        # and create an sde connection file, set as current workspace
        # out_folder, platform, instance, target_sde, version_name, new_name, parent_version, log
        version_manager = VersionManager(out_f, plat, inst, sde_file, v_name, n_name, p_version, log_name)
        version_manager.clean_previous()
        version_sde_file = version_manager.connect_version()

        # create WeaverUpdater class object
        weaver_updater = WeaverUpdater(w_table, add_rows, exist_rows, version_sde_file)

        # get the number for rows per parcel ID
        pid_dict = weaver_updater.count_pid()
        # should return True when editing is complete
        table_updated = weaver_updater.perform_update()

        version_manager = VersionManager(out_f, plat, inst, sde_file, v_name, n_name, p_version, log_name)
        version_manager.clean_previous()
        version_sde_file = version_manager.connect_version()
        dataset_workspace = arcpy.ListDatasets("*{}*".format(dataset))[0]
        buildings = arcpy.ListFeatureClasses("*{}*".format(bldgs), "Polygon", dataset_workspace)[0]
        # create BuildingUpdater class object
        building_updater = BuildingsUpdater(buildings, w_table, folio_num_field, update_atts, version_sde_file)
        folios = building_updater.get_folios()
        # should return True when editing it complete
        buildings_updated = building_updater.update_buildings()
        # move edits to the default version, and delete the noisemit version
        version_manager.rec_post()
    else:
        print "The files are identical, no edits needed"

except:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)




