import arcpy
import os
import sys
import logging
from arcpy import da
from arcpy import env
import traceback
env.overwriteOutput = 1

home_dir = os.path.dirname(os.path.abspath(__file__))


def compare_tables(sql_table, gdb_table):
    """Compare the fields between the tables to catch a schema change.
    List all of the rows in the source table, check for the existence of an existing row
    in the list of source rows.

    The return value is a dictionary including the folio Ids for rows being updated,
    as well as the rows themselves.

    If no changes need to be made, the 'compare_result' value in the result dict will be zero."""

    # verify that the necessary tables exist
    for x in [sql_table, gdb_table]:
        if not arcpy.Exists(x):
            arcpy.AddError("the target table and the gdb table are not found")
            raise Exception()

    source_fields = {}
    read_fields = arcpy.ListFields(sql_table)
    for x in read_fields:
        source_fields[x.name] = x.type

    target_fields = {}
    existing_fields = arcpy.ListFields(gdb_table)
    for x in existing_fields:
        target_fields[x.name] = x.type

    # The only missing field should be ObjectID because the sql table is not registered with the geodatabase
    new_fields = [f for f in source_fields.keys() if f not in target_fields.keys()]
    missing_fields = [f for f in target_fields.keys() if f not in source_fields.keys()]

    _match_fields = [f for f in target_fields.keys() if f in source_fields.keys()]
    if "OBJECTID" in _match_fields:
        _match_fields.remove("OBJECTID")

    folio_index = []
    if "FolioNumber" in _match_fields:
        folio_index.append(_match_fields.index("FolioNumber"))

    if len(new_fields):
        raise Exception("A schema change exists in the updated weaver table :: {}".format(new_fields))

    if missing_fields != [u'OBJECTID'] and missing_fields != []:
        arcpy.AddWarning("The updated weaver table is missing fields :: {}\
                        A Schema change may be needed to import the table,\
                        or else the column will be empty".format(missing_fields))

    # Add all of the rows from the weaver sql table to a list
    add_rows = []
    with da.SearchCursor(sql_table, _match_fields) as cursor:
        for row in cursor:
            row = clean_row(row)
            add_rows.append(row)
    del cursor

    rem_rows = []
    with da.SearchCursor(gdb_table, _match_fields) as cursor:
        for row in cursor:
            row = clean_row(row)
            # the rows from the GDB table, are identical to any row in the list, remove that row from the list
            if row in add_rows:
                add_rows.remove(row)
                pass
            # if the row is not in the add_rows, then add it to the exist_rows to remove
            else:
                rem_rows.append(row)
    del cursor

    compare_result = 0
    folioIds = []
    if len(add_rows) or len(rem_rows):
        compare_result += 1
        for x in add_rows:
            try:
                folioIds.append(x[folio_index[0]])
            except IndexError:
                pass
        for x in rem_rows:
            try:
                folioIds.append(x[folio_index[0]])
            except IndexError:
                pass
    folioIds = list(set(folioIds))
    arcpy.AddMessage("These {} folioIds will be updated from the weaver table :: {}".format(len(folioIds), folioIds))
    return {"match_fields": _match_fields,
            "folioIds": folioIds,
            "compare_result": compare_result,
            "add_rows": add_rows,
            "exist_rows": rem_rows}


def print_connection_info(workspace):
    """print the connection properties of the workspace describe object"""
    x = arcpy.Describe(workspace)
    cp = x.connectionProperties
    print cp.database, cp.version


def clean_row(_row):
    """take an input row from a cursor, clean it, then return the cleaned row"""
    cleaned_row = []
    for _x in _row:
        try:
            _x = _x.strip()
        except AttributeError:
            pass
        cleaned_row.append(_x)
    return cleaned_row


class VersionException(Exception):
    """throw this when handling exception with managing versions"""
    def __init__(self, *args):
        exc_t, exc_v, exc_trace = sys.exc_info()
        traceback.print_exception(exc_t, exc_v, exc_trace,
                                  limit=2, file=sys.stdout)


class SdeConnector:
    def __init__(self, out_f, out_name, platform, instance, options):
        self.out_folder = out_f
        self.connection_name = out_name
        self.platform = platform
        self.instance = instance
        self.options = options

    def create_sde_connection(self):
        arcpy.AddMessage("SdeConnection.create_sde_connection()")
        # delete the sde file if it exists
        loc = os.path.join(self.out_folder, self.connection_name)
        if os.path.exists(loc):
            os.remove(loc)

        sd = arcpy.CreateDatabaseConnection_management(self.out_folder, self.connection_name, self.platform, self.instance,
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
    def __init__(self, opt, connection_folder, target_sde, new_version, new_connection, platform, instance):

        self.opt = opt
        self.connection_folder = connection_folder
        self.target_sde = target_sde
        parent_version = opt["version"]
        self.version_name = u"{}.{}".format(parent_version.split(".")[0].upper(), new_version)
        self.new_version = new_version
        self.new_connection = new_connection
        self.version_sde = ""
        self.parent_version = parent_version
        self.platform = platform
        self.instance = instance
        pass

    def clean_previous(self):
        arcpy.AddMessage("VersionManager.clean_previous()")
        # remove previous version if it exists
        versions = []
        try:
            if arcpy.Exists(self.target_sde):
                env.workspace = self.target_sde
                versions.extend(da.ListVersions(self.target_sde))
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
        arcpy.AddMessage("VersionManager.connect_version()")
        # create version to edit
        versions = da.ListVersions(self.target_sde)
        if len(versions):
            v_names = [v.name for v in versions]
            if self.version_name in v_names:
                raise VersionException("Version already exists, must remove before proceeding")
            else:
                try:
                    arcpy.CreateVersion_management(self.target_sde, self.parent_version, self.new_version,
                                                   access_permission="PUBLIC")
                    # create an sde connection file to the new version
                    v_opt = self.opt.copy()

                    versions = da.ListVersions(self.target_sde)
                    edit_version = []
                    for x in versions:
                        if self.new_version in x.name:
                            edit_version.append(x.name)

                    v_opt["version"] = edit_version[0]
                    arcpy.AddMessage("Edit Version selected :: {}".format(edit_version[0]))

                    # create SdeConnector object for the version
                    # out_folder, out_name, platform, instance, options
                    version_connection = SdeConnector(self.connection_folder, self.new_connection, self.platform,
                                                      self.instance, v_opt)
                    self.version_sde = version_connection.create_sde_connection()
                    return self.version_sde

                except Exception as e:
                    raise VersionException(e.message)

        else:
            raise VersionException()

    def rec_post(self):
        arcpy.AddMessage("VersionManager.rec_post()")
        try:
            # Block additional connections during rec/post
            env.workspace = self.version_sde
            logfile = os.path.join(home_dir, "logs\\NoiseMit_logfile.txt")
            arcpy.ReconcileVersions_management(self.version_sde, "ALL_VERSIONS", u"{}".format(self.parent_version),
                                               u"{}".format(self.version_name), "NO_LOCK_ACQUIRED",
                                               "ABORT_CONFLICTS", "BY_OBJECT", "FAVOR_TARGET_VERSION",
                                               "POST", "DELETE_VERSION", logfile)
            os.remove(self.version_sde)
            return True
        except Exception as e:
            raise VersionException("Unable to rec/post edits :: {}".format(e.message))


class GDBTableUpdater:
    """match_fields, w_table, add_rows, rem_rows, version_sde_file, editor"""
    def __init__(self, weaver_attributes, folioIds, match_fields, write_table, read_rows, remove_rows, version_sde, editor):
        self.folio_field = weaver_attributes["Folio Number"]
        self.folioIds = folioIds
        self.match_fields = match_fields
        self.write_table = write_table
        self.read_rows = read_rows
        self.remove_rows = remove_rows
        self.version_sde = version_sde
        self.editor = editor
        pass

    def insert_rows(self):
        arcpy.AddMessage("GDBTableUpdater.insert_rows()")
        try:
            self.editor.startOperation()
            insert = da.InsertCursor(self.write_table, self.match_fields)
            i = 0
            pre_cnt = int(arcpy.GetCount_management(self.write_table).getOutput(0))
            for _row in self.read_rows:
                try:
                    insert.insertRow(_row)
                    i += 1
                except Exception as e:
                    print e.message

            del insert
            if not i:
                arcpy.AddWarning("Rows were not added to the GDB Table")
            else:
                arcpy.AddMessage("{} rows were added to the GDB Table".format(i))
            self.editor.stopOperation()
            return i

        except Exception as h:
            print h.message
            self.editor.stopOperation()

    def delete_rows(self, folio_ids):
        arcpy.AddMessage("GDBTableUpdater.delete_rows()")
        try:
            self.editor.startOperation()
            i = 0
            rem_rows = self.remove_rows
            with da.UpdateCursor(self.write_table, self.match_fields, "{} in ('{}')".format(
                    self.folio_field, "','".join(folio_ids))) as _cursor:
                for line in _cursor:
                    # delete all rows that are identical to an item in the input list
                    line = clean_row(line)
                    if line in rem_rows:
                        i += 1
                        _cursor.deleteRow()
                        pass
                    else:
                        pass
            del _cursor
            if not i:
                arcpy.AddWarning("Rows were not removed from the GDB Table")
            else:
                arcpy.AddMessage("{} rows were removed from the GDB Table".format(i))
            self.editor.stopOperation()
            return i

        except Exception as e:
            print e.message
            self.editor.stopOperation()

    def update_table(self, folio_ids):
        arcpy.AddMessage("GDBTableUpdater.update_table()")
        try:
            # use the update cursor to remove the rem_rows
            deleted, added = 0, 0
            if len(self.remove_rows):
                deleted = self.delete_rows(folio_ids)
            if len(self.read_rows):
                added = self.insert_rows()
            return [deleted, added]

        except Exception as e:
            arcpy.AddError(e.message)

    def perform_update(self):
        arcpy.AddMessage("GDBTableUpdater.perform_update()")
        try:
            # if the table is empty add all read_rows
            if not int(arcpy.GetCount_management(self.write_table).getOutput(0)):
                self.insert_rows()
            else:
                # use the folioIds to filter before updating
                self.update_table(self.folioIds)
            return True
        except Exception as e:
            arcpy.AddError(e.message)


class BuildingsUpdater:
    """buildings, w_table, building_attributes, weaver_attributes,version_sde_file, editor"""
    def __init__(self, folioIds, bldgs, rel_table, bldg_atts, weav_atts, version_sde, editor):
        self.folioIds = folioIds
        self.buildings = bldgs
        self.rel_table = rel_table
        self.bldg_folio = bldg_atts["Folio Number"]
        self.bldg_update_fields = [bldg_atts["Project Name"], bldg_atts["Phase Name"]]
        self.weav_folio = weav_atts["Folio Number"]
        self.weav_update_fields = [weav_atts["Project Name"], weav_atts["Phase Name"]]
        self.version_sde = version_sde
        self.editor = editor
        self.folios = {}

    def build_folio_dict(self):
        for x in self.folioIds:
            self.folios[x] = {"Phase Names": [], "Project Names": []}
        return self.folios

    def update_buildings(self):
        arcpy.AddMessage("BuildingUpdater.update_buildings()")
        # gather the building folio numbers as keys in a dict

        def concat_list(_input):
            """take the input multivalue list and output a string"""
            _ph = _input
            if type(_ph) == list:
                _ph = list(set(_ph))
                _ph = ", ".join(_ph)
            return _ph

        _folios = self.build_folio_dict()
        fields = [self.weav_folio, self.weav_update_fields[0], self.weav_update_fields[1]]
        # read the rows from the related table with an SQL filter for folioIds;
        # add the phase name and project name to the values for the
        # dictionary index of the folio number
        keys = _folios.keys()
        run = 0
        if len(keys) == 1:
            sql_expression = "{} = '{}'".format(fields[0], keys[0])
            run += 1
        elif len(keys) > 1:
            sql_expression = "{} in ('{}')".format(fields[0], "','".join(keys))
            run += 1
        else:
            arcpy.AddMessage("keys: {}".format(len(keys)))

        if run:
            try:
                with da.SearchCursor(self.rel_table, fields, sql_expression) as _cursor:
                    for _row in _cursor:
                        cleaned_row = clean_row(_row)
                        if cleaned_row[0] in _folios:
                            _folios[cleaned_row[0]]["Phase Names"].append(cleaned_row[1])
                            _folios[cleaned_row[0]]["Project Names"].append(cleaned_row[2])

                del _cursor

            except RuntimeError as e:
                print e.message

            fields = [self.bldg_folio, self.bldg_update_fields[0], self.bldg_update_fields[1]]
            if len(keys) == 1:
                sql_expression = "{} = '{}'".format(fields[0], keys[0])
            elif len(keys) > 1:
                sql_expression = "{} in ('{}')".format(fields[0], "','".join(keys))
            # iterate through the buildings with an SQL filter for the folioIds being updated;
            # for each folio number create the new string value of the concatenated
            # phase names and project names
            arcpy.AddMessage("The buildings are now being updated")
            i = 0
            self.editor.startOperation()
            with da.UpdateCursor(self.buildings, fields, sql_expression) as _cursor:
                for _row in _cursor:
                    cleaned_row = clean_row(_row)
                    folio_id = cleaned_row[0]
                    if folio_id in keys:
                        ph = _folios[folio_id]["Phase Names"]
                        phase_name = concat_list(ph)

                        pn = _folios[folio_id]["Project Names"]
                        project_name = concat_list(pn)

                        new_row = [folio_id, phase_name, project_name]
                        if _row != new_row:
                            try:
                                _cursor.updateRow(new_row)
                                i += 1
                            except:
                                pass
                        else:
                            # the row has not changed
                            pass
                    else:
                        print "folio# {} was not found in the related table".format(folio_id)
            del _cursor
            self.editor.stopOperation()
            arcpy.AddMessage("{} buildings were updated with values".format(i))
        else:
            arcpy.AddWarning("Buildings were not updated.")
        return True






