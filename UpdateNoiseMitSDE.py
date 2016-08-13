import arcpy
import os
import sys
import logging
from arcpy import da
from arcpy import env
from collections import Counter
import traceback
env.overwriteOutput = 1


def compare_fields(sql_table, existing_table):
    """Compare the fields between the tables to catch a schema change"""
    read_fields = arcpy.ListFields(sql_table)
    read_field_names = [f.name for f in read_fields]

    existing_fields = arcpy.ListFields(existing_table)
    existing_field_names = [f.name for f in existing_fields]

    # The only missing field should be ObjectID because the sql table is not registered with the geodatabase
    new_fields = [f for f in read_field_names if f not in existing_field_names]
    missing_fields = [f for f in existing_field_names if f not in read_field_names]

    _match_fields = [f for f in existing_field_names if f in read_field_names]
    if "OBJECTID" in _match_fields:
        _match_fields.remove("OBJECTID")

    if len(new_fields):
        raise Exception("A schema change exists in the updated weaver table :: {}".format(new_fields))

    if missing_fields != [u'OBJECTID'] and missing_fields != []:
        arcpy.AddWarning("The updated weaver table is missing fields :: {}\
                        A Schema change may be needed to import the table,\
                        or else the column will be empty".format(missing_fields))

    # verify that the necessary tables exist
    for x in [sql_table, existing_table]:
        if not arcpy.Exists(x):
            arcpy.AddError("the target table and the building are not found")
            raise Exception()

    # Add all of the rows from the weaver sql table to a list
    add_rows = []
    with da.SearchCursor(sql_table, _match_fields) as cursor:
        for row in cursor:
            tuple_row = tuple([u"{}".format(x) for x in row])
            add_rows.append(tuple_row)
    del cursor

    exist_rows = []
    with da.SearchCursor(existing_table, _match_fields) as cursor:
        for row in cursor:
            tuple_row = tuple([u"{}".format(x) for x in row])
            # the rows from the GDB table, are identical to any row in the list, remove that row from the list
            if tuple_row in add_rows:
                add_rows.remove(tuple_row)
                pass
            # if the row is not in the add_rows, then add it to the exist_rows to remove
            else:
                exist_rows.append(row)
    del cursor

    compare_result = 0
    if len(add_rows) or len(exist_rows):
        compare_result += 1

    return {"match_fields": _match_fields,
            "compare_result": compare_result,
            "add_rows": add_rows,
            "exist_rows": exist_rows}


def print_connection_info(workspace):
    """print the connection properties of the workspace describe object"""
    x = arcpy.Describe(workspace)
    cp = x.connectionProperties
    print cp.database, cp.version


def clean_row(_row):
    """take an input row from a cursor, clean it, then return the cleaned row"""
    cleaned_row = []
    for _x in _row:
        if _x is None:
            _x = "unknown"
        _x = _x.strip()
        cleaned_row.append(_x)
    return cleaned_row


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

        if arcpy.Exists(target_sde):
            d = target_sde
        else:
            d = False
        return d


class VersionManager:
    def __init__(self, opt, out_folder, uid, platform, instance, target_sde, new_name, parent_version):
        self.opt = opt
        self.instance = instance
        self.platform = platform
        self.out_folder = out_folder
        self.target_sde = target_sde
        self.version_name = "{}.{}".format(uid.upper(), new_name)
        self.new_name = new_name
        self.version_sde = ""
        self.parent_version = parent_version
        pass

    def clean_previous(self):
        # remove previous version if it exists
        versions = []
        try:
            if arcpy.Exists(self.target_sde):
                env.workspace = self.target_sde
                versions = da.ListVersions(self.target_sde)
        except EnvironmentError as e:
            raise VersionException(e.message)

        if len(versions):
            v_names = [v.name for v in versions]
            if self.version_name in v_names:
                try:
                    arcpy.DeleteVersion_management(self.target_sde, self.version_name)
                    return True
                except:
                    raise VersionException()
            elif len(v_names) == 1:
                return True
            else:
                logging.warning("""There are versions existing in the geodatabase
                                 that may need to be removed:: {}""".format(v_names))
        else:
            logging.info("no versions found")
            return True

    def connect_version(self):
        # create version to edit
        versions = da.ListVersions(self.target_sde)
        if len(versions):
            v_names = [v.name for v in versions]
            if self.version_name in v_names:
                raise VersionException("Version already exists, must remove before proceeding")
            else:
                try:
                    arcpy.CreateVersion_management(self.target_sde, self.parent_version, self.new_name,
                                                   access_permission="PUBLIC")
                except Exception as e:
                    raise VersionException(e.message)


            # create an sde connection file to the new version
            v_opt = self.opt.copy()
            v_opt["version"] = self.version_name

            # create SdeConnector object for the version
            version_connection = SdeConnector(self.out_folder, self.new_name, self.platform, self.instance, v_opt)
            self.version_sde = version_connection.create_sde_connection()
            return self.version_sde

        else:
            raise VersionException()

    def rec_post(self):
        try:
            # Block additional connections during rec/post
            env.workspace = self.version_sde

            arcpy.ReconcileVersions_management(self.version_sde, "ALL_VERSIONS", u"{}".format(self.parent_version),
                                               u"{}".format(self.version_name), "NO_LOCK_ACQUIRED",
                                               "ABORT_CONFLICTS", "BY_OBJECT", "FAVOR_TARGET_VERSION",
                                               "POST", "DELETE_VERSION", "C:\\Temp\NoiseMit_logfile.txt")
            os.remove(self.version_sde)
            return True
        except Exception as e:
            raise VersionException("Unable to rec/post edits :: {}".format(e.message))


class GDBTableUpdater:
    """match_fields, w_table, add_rows, rem_rows, version_sde_file, editor"""
    def __init__(self, match_fields, write_table, read_rows, remove_rows, version_sde, editor):
        self.match_fields = match_fields
        self.write_table = write_table
        self.read_rows = read_rows
        self.remove_rows = remove_rows
        self.version_sde = version_sde
        self.items = {}
        self.editor = editor
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

    def insert_rows(self):
        try:
            self.editor.startOperation()

            insert = da.InsertCursor(self.write_table, self.match_fields)
            i = 0
            for _row in self.read_rows:
                insert.insertRow(_row)
                i += 1
            del insert

            self.editor.stopOperation()
            return i

        except Exception as h:
            print h.message

    def delete_rows(self, parcel_id):
        self.editor.startOperation()
        i = 0
        with da.UpdateCursor(self.write_table, self.match_fields, "ParcelID='{}'".format(parcel_id)) as _cursor:
            for line in _cursor:
                # delete all rows that are identical to an item in the input list
                if line in self.remove_rows:
                    _cursor.deleteRow(line)
                    i += 1
        del _cursor
        self.editor.stopOperation()
        return i

    def update_table(self, parcel_id):
        try:
            # use the update cursor to remove the rem_rows
            deleted, added = 0, 0
            if len(self.remove_rows):
                deleted = self.delete_rows(parcel_id)
            if len(self.read_rows):
                added = self.insert_rows()
            return [deleted, added]

        except Exception as e:
            arcpy.AddError(e.message)

    def perform_update(self):

        try:
            # if the table is empty add all read_rows
            if not int(arcpy.GetCount_management(self.write_table).getOutput(0)):
                self.insert_rows()
            else:
                # work with one pid at a time
                for pid in self.items.keys():
                    self.update_table(pid)
        except Exception as e:
            arcpy.AddError(e.message)


class BuildingsUpdater:
    """buildings, w_table, building_attributes, weaver_attributes,version_sde_file, editor"""
    def __init__(self, b, rel_table, bldg_atts, weav_atts, version_sde, editor):
        self.buildings = b
        self.rel_table = rel_table
        self.bldg_folio = bldg_atts["Folio Number"]
        self.bldg_update_fields = [bldg_atts["Project Name"], bldg_atts["Phase Name"]]
        self.weav_folio = weav_atts["Folio Number"]
        self.weav_update_fields = [weav_atts["Project Name"], weav_atts["Phase Name"]]
        self.version_sde = version_sde
        self.editor = editor
        self.folios = {}

    def get_folios(self):
        with da.SearchCursor(self.buildings, self.bldg_folio) as _cursor:
            for _row in _cursor:
                cleaned_row = clean_row(_row)
                self.folios[cleaned_row[0]] = {"Phase Names": [], "Project Names": []}
        del _cursor
        return self.folios

    def update_buildings(self):
        # gather the building folio numbers as keys in a dict
        def concat_list(_input):
            """take the input multivalue list and output a string"""
            _ph = _input
            if type(_ph) == list:
                cnt = Counter()
                for w in _ph:
                    cnt[w] += 1

                _ph = ""
                if len(cnt.items()):
                    l = []
                    d = dict(cnt)

                    for k, v in d.iteritems():
                        l.append("{}:{}".format(k, v))
                    if len(l) > 1:
                        _ph = ", ".join(l)
                    else:
                        _ph = str(l[0])

            return _ph

        _folios = self.get_folios()
        fields = [self.weav_folio, self.weav_update_fields[0], self.weav_update_fields[1]]
        with da.SearchCursor(self.rel_table, fields) as _cursor:
            for _row in _cursor:
                cleaned_row = clean_row(_row)
                if cleaned_row[0] in _folios:
                    _folios[cleaned_row[0]]["Phase Names"].append(cleaned_row[1])
                    _folios[cleaned_row[0]]["Project Names"].append(cleaned_row[2])

        del _row
        del _cursor

        self.editor.startOperation()

        fields = [self.bldg_folio, self.bldg_update_fields[0], self.bldg_update_fields[1]]
        with da.UpdateCursor(self.buildings, fields) as _cursor:
            for _row in _cursor:
                cleaned_row = clean_row(_row)
                folio_id = cleaned_row[0]
                if folio_id in _folios:
                    ph = _folios[folio_id]["Phase Names"]
                    phase_name = concat_list(ph)

                    pn = _folios[folio_id]["Project Names"]
                    project_name = concat_list(pn)

                    new_row = [folio_id, phase_name, project_name]
                    _cursor.updateRow(new_row)
                else:
                    print "{} is not in the weaver table".format(folio_id)
        del _row
        del _cursor

        self.editor.stopOperation()
        return True






