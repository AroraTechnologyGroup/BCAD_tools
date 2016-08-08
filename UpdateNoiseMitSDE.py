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


def create_memory_tables(read_table, write_table):
    # create variables for table and building feature class memory objects
    memory_read_table = {"in_memory\\weaver": read_table}
    memory_write_table = {"in_memory\\noisemit": write_table}
    d = {}
    # Delete the in memory objects if they were not cleaned during previous run
    for k, v in [memory_read_table, memory_write_table]:
        if arcpy.Exists(k):
            arcpy.Delete_management(k)
            layer = arcpy.MakeTableView_management(v, k)
            d[k.split("\\")[-1]] = layer

    return d


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
bldgs = r"DEVELOPMENT_BCAD.DBO.NoiseMitigation\\DEVELOPMENT_BCAD.DBO.Building_Information"
update_atts = ["PROJECTNAM", "PHASENAME"]
folio_num_field = "FOLIONUMBE"


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
    def __init__(self, write_table, read_table, version_sde):
        self.write_table = write_table
        self.read_table = read_table
        self.version_sde = version_sde
        self.items = {}
        self.all_fields = []
        pass

    def count_pid(self):
        parcel_ids = []
        read_cursor = da.SearchCursor(self.read_table, 'ParcelID')
        for _row in read_cursor:
            parcel_ids.append(_row[0])
        del read_cursor

        _cnt = Counter()
        for _pid in parcel_ids:
            _cnt[_pid] += 1

        self.items = dict(_cnt)
        return self.items

    @staticmethod
    def get_rows(in_table, _pid):
        reader = da.SearchCursor(in_table, "*", "ParcelID='{}'".format(_pid))
        # rows contains all of the rows with the pid
        rows = []
        for row in reader:
            rows.append(row)
        del reader
        return rows

    def insert_rows(self, input_rows):
        try:
            edit.startOperation()

            insert = da.InsertCursor(self.write_table, self.all_fields)

            for _row in input_rows:
                insert.insertRow(_row)
            del insert

            edit.stopOperation()

        except Exception as h:
            print h.message

    @staticmethod
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
        n_row = []

        for tup in zipped:
            new_row.append(tup[1])
            if tup[0] != tup[1]:
                _replace += 1

        return _replace, n_row

    def swap_rows(self, input_rows, _rem_rows, parcel_id, editor):
        for _row in input_rows:
            index = input_rows.index(_row)
            replace = 0
            editor.startOperation()
            with da.UpdateCursor(self.write_table, self.all_fields, "ParcelID='{}'".format(parcel_id)) as _cursor:
                for line in _cursor:
                    # update all rows from the remove_rows list
                    if line in _rem_rows:
                        replace, n_row = self.compare_cells(replace, index, line, input_rows)
                        if replace:
                            _cursor.updateRow(n_row)
                            break
            editor.stopOperation()

    def delete_rows(self, _rem_rows, parcel_id, editor):
        editor.startOperation()
        with da.UpdateCursor(self.write_table, self.all_fields, "ParcelID='{}'".format(parcel_id)) as _cursor:
            for line in _cursor:
                # delete all rows from the input list
                if line in _rem_rows:
                    _cursor.deleteRow(line)
        editor.startOperation()

    def update_table(self, parcel_id, n_rows, remove_rows, editor):
        try:
            # use the update cursor to update the rem_rows with the in_rows values
            if len(n_rows) == len(remove_rows):
                self.swap_rows(n_rows, remove_rows, parcel_id, editor)
                pass

            elif len(n_rows) > len(remove_rows):
                sl1 = n_rows[:len(remove_rows)]
                sl2 = n_rows[len(remove_rows):]

                self.swap_rows(sl1, remove_rows, parcel_id, editor)

                self.insert_rows(sl2)

            elif len(n_rows) < len(remove_rows):
                sl1 = remove_rows[:len(n_rows)]
                sl2 = remove_rows[len(n_rows):]

                self.swap_rows(n_rows, sl1, parcel_id, editor)

                self.delete_rows(sl2, parcel_id, editor)

        except Exception as f:
            print f.message

        return

    def perform_update(self):
        # begin edit session
        edit = da.Editor(self.version_sde)
        edit.startEditing()
        # work with one pid at a time
        for pid in self.items.keys():
            # number of rows that should exist in the target table for the pid
            cnt = self.items[pid]
            # pull the rows into a list
            read_rows = self.get_rows(self.read_table, pid)
            exist_rows = self.get_rows(self.write_table, pid)
            read_rows.sort()
            exist_rows.sort()

            in_rows = [r for r in read_rows if r not in exist_rows]
            rem_rows = [r for r in exist_rows if r not in read_rows]

            t_fields = arcpy.ListFields(self.write_table)
            t_fields = [t.name for t in t_fields]
            t_fields.remove('OBJECTID')
            self.all_fields = t_fields

            # table is empty, populate it
            if len(exist_rows) == 0:
                edit.startOperation()
                self.insert_rows(read_rows)
                edit.stopOperation()
                pass
            # update the rows that are not identical, a complex function
            else:
                self.update_table(pid, in_rows, rem_rows, edit)

        edit.stopEditing(True)
        return True


class BuildingsUpdater:
    def __init__(self, buildings, rel_table, update_fields, folio_field, version_sde):
        self.buildings = buildings
        self.version_sde = version_sde
        building_w_space = os.path.basename(os.path.dirname(arcpy.Describe(buildings).catalogPath))
        self.workspace = os.path.join(version_sde, building_w_space)
        self.folio_field = folio_field
        self.update_fields = update_fields
        self.rel_table = rel_table
        self.folios = []

    def get_folios(self):
        with da.SearchCursor(self.buildings, [self.folio_field]) as cursor:
            for row in cursor:
                self.folios.append(row[0])

        return self.folios

    def update_buildings(self):
        # gather the building folio numbers
        _folios = self.get_folios()
        edit = da.Editor(self.workspace)
        edit.startEditing()
        for folio in _folios:
            matrix = []
            with da.SearchCursor(self.rel_table, self.update_fields, "{}='{}'".format(self.folio_field, folio)) as cursor:
                for row in cursor:
                    matrix.append(row)
            comps = list(zip(*matrix))
            edit.startOperation()
            with da.UpdateCursor(self.buildings, self.update_fields, "{}='{}'".format(self.folio_field, folio)) as cursor:
                for row in cursor:
                    new_row = []
                    for x in comps:
                        s = set(x)
                        if len(s) > 1:
                            new_row.append(", ".join(s))
                        elif len(s) == 1:
                            new_row.append(s.pop())
                    cursor.updateRow(new_row)
            edit.stopOperation()
        edit.stopEditing(True)
        return True

try:
    #cnxn = create_sql_connection(driver, server, port, database, uid, pwd)
    connection = SdeConnector(out_f, out_n, plat, inst, opt)
    sde_file = connection.create_sde_connection()

    r_table, w_table = compare_fields("*weaver_formatted", "*WEAVER")

    # verify that the necessary tables exist
    for x in [r_table, w_table, bldgs]:
        if not arcpy.Exists(x):
            logging.error("the target table and the building are not found")
            raise Exception()

    # Check if read table and write table are identical
    mem_objects = create_memory_tables(r_table, w_table)
    
    compare_result = arcpy.FeatureCompare_management(mem_objects['weaver'], mem_objects['noisemit'], folio_num_field, "ATTRIBUTES_ONLY",
                                                     ["IGNORE_RELATIONSHIPCLASSES","IGNORE_FIELDALIAS"],
                                                     omit_field="OBJECTID", continue_compare="NO_CONTINUE_COMPARE")
    if compare_result.getOutput(0):
        # create VersionManager class object to create new version, connect to it,
        # and create an sde connection file, set as current workspace
        # out_folder, platform, instance, target_sde, version_name, new_name, parent_version, log
        version_manager = VersionManager(out_f, plat, inst, sde_file, v_name, n_name, p_version, log_name)
        version_manager.clean_previous()
        version_sde_file = version_manager.connect_version()

        # create WeaverUpdater class object
        weaver_updater = WeaverUpdater(w_table, r_table, version_sde_file)

        # get the number for rows per parcel ID
        pid_list = weaver_updater.count_pid()
        # should return True when editing is complete
        table_updated = weaver_updater.perform_update()

        # create BuildingUpdater class object
        building_updater = BuildingsUpdater(bldgs, w_table, update_atts, folio_num_field, version_sde_file)
        folios = building_updater.get_folios()
        # should return True when editing it complete
        buildings_updated = building_updater.update_buildings()
        # move edits to the default version, and delete the noisemit version
        version_manager.rec_post()
    else:
        print "The files are identical, no edits needed"

except Exception as e:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)




