import arcpy
import os
import sys
import logging
from arcpy import da
from arcpy import env
import traceback
import datetime
from collections import Counter
import json
env.overwriteOutput = 1

home_dir = os.path.dirname(os.path.abspath(__file__))

logger = logging.getLogger(__package__)

domain_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "domains.json")
file = open(domain_file, 'r')
domains = json.loads(file.read())
file.close()


def compare_tables(sql_table, gdb_table):
    arcpy.AddMessage("UpdateNoiseMitSDE.compare_tables()")
    """
    1. Compare the fields between the tables to catch a schema change.
    2. Perform the field type and length filter on the read rows to remove values that are invalid in the target table

    The return value is a dictionary including the folio Ids for rows being updated,
    as well as the rows themselves.

    If no changes need to be made, the 'compare_result' value in the result dict will be zero."""
    try:
        # verify that the necessary tables exist
        for x in [sql_table, gdb_table]:
            ingdb = "\\".join(x.split("\\")[:-1])
            env.workspace = ingdb
            table_name = x.split("\\")[-1]
            if not arcpy.Exists(table_name):
                arcpy.AddError("the target table and the gdb table are not found")
                raise Exception()
            else:
                pass
        source_fields = {}
        read_fields = arcpy.ListFields(sql_table)
        for x in read_fields:
            source_fields[x.name.lower()] = {
                "type": x.type,
                "name": x.name
            }

        target_fields = {}
        existing_fields = arcpy.ListFields(gdb_table)
        for x in existing_fields:
            if x.type == "String":
                length = x.length
            else:
                length = None

            target_fields[x.name.lower()] = {
                "type": x.type,
                "name": x.name,
                "length": length
            }

        # The only missing field should be ObjectID because the sql table is not registered with the geodatabase
        source_keys = list(source_fields.keys())
        target_keys = list(target_fields.keys())

        new_fields = []
        new_fields.extend([f for f in source_keys if f not in target_keys])
        missing_fields = []
        missing_fields.extend([f for f in target_keys if f not in source_keys])
        _match_fields = []
        _match_fields.extend([f for f in target_keys if f in source_keys])

        # these attributes will differ between the tables and break the matching
        remove_fields = [u"OBJECTID", u"DateStamp", u"LastScannedDate", u"Shape", u"Shape.STArea()",
                         u"last_edited_date", u"last_edited_user", u"Shape.STLength()"]

        for x in remove_fields:
            value = x.lower()
            if value in _match_fields:
                _match_fields.remove(value)
            if value in missing_fields:
                missing_fields.remove(value)
            if value in new_fields:
                new_fields.remove(value)

        # for the remaining match fields, compare their types.
        for x in _match_fields:
            source_type = source_fields[x]["type"]
            target_type = target_fields[x]["type"]
            if source_type != target_type:
                logger.error("{} source type != {} target type".format(source_type, target_type))

        folio_index = []
        if "folionumber" in _match_fields:
            folio_index.append(_match_fields.index("folionumber"))

        if len(new_fields):
            raise Exception("A schema change is needed in the updated {} table :: {}".format(gdb_table, new_fields))

        if len(missing_fields):
            arcpy.AddMessage("These fields were not found in the source sql table :: {}".format(missing_fields))

        # Add all of the rows from the weaver sql table to a list

        field_names = [source_fields[y]["name"] for y in _match_fields]
        add_rows = []
        with da.SearchCursor(sql_table, field_names) as cursor:
            for row in cursor:
                row = clean_row(target_fields, field_names, row)
                i = 0
                for x in row:
                    # this removes empty rows from the source list
                    if x:
                        i += 1
                    if i:
                        break
                if i:
                    add_rows.append(row)
        del cursor

        field_names = [target_fields[y]["name"] for y in _match_fields]
        rem_rows = []
        with da.SearchCursor(gdb_table, field_names) as cursor:
            for row in cursor:
                row = clean_row(target_fields, field_names, row)
                if row in add_rows:
                    add_rows.remove(row)
                    pass
                # if the row is not in the add_rows, then add it to the exist_rows to remove
                else:
                    rem_rows.append(row)
        del cursor

        compare_result = 0
        folioId_dict = {
            "rem": [],
            "add": []
        }
        if len(add_rows) or len(rem_rows):
            compare_result += 1
            for x in add_rows:
                try:
                    # If folioIds are used, or some other ID, then use that to index the rows, other wise use the entire row
                    if len(folio_index):
                        folioId_dict["add"].append(x[folio_index[0]])
                    else:
                        folioId_dict["add"].append(x)
                except IndexError:
                    pass
            for x in rem_rows:
                try:
                    if len(folio_index):
                        folioId_dict["rem"].append(x[folio_index[0]])
                    else:
                        folioId_dict["rem"].append(x)
                except IndexError:
                    pass

        folioIds = []
        if len(folio_index):
            folioIds.extend(folioId_dict["rem"])
            folioIds.extend(folioId_dict["add"])
            folioIds = list(set(folioIds))

        if len(folioIds):
            arcpy.AddMessage("{} folioIds will be updated in the table".format(len(folioIds)))
            arcpy.AddMessage("{} folios are flagged for removal".format(len(set(folioId_dict["rem"]))))
            arcpy.AddMessage("{} folios are flagged for insert".format(len(set(folioId_dict["add"]))))

        else:
            # folioIds are not used to look up records, the entire row is added as an item
            arcpy.AddMessage("{} rows will be removed from the table".format(len(folioId_dict["rem"])))
            arcpy.AddMessage("{} rows will be added to the table".format(len(folioId_dict["add"])))

        return {
            "match_fields": _match_fields,
            "folioIds": folioIds,
            "compare_result": compare_result,
            "add_rows": add_rows,
            "exist_rows": rem_rows
        }

    except Exception as e:
        print(e)


def print_connection_info(workspace):
    arcpy.AddMessage("UpdateNoiseMitSDE.print_connection_info()")
    """print the connection properties of the workspace describe object"""
    x = arcpy.Describe(workspace)
    cp = x.connectionProperties
    print(cp)
    return {"database": cp.database, "version": cp.version}


def clean_row(target_fields, field_names, _row):
    """take an input row from a cursor, clean it, compare against target schema, then return the cleaned row"""
    """
    Cleaning the rows should only happen once when the data is read from the source table,  when the buildings 
    are updated from the related table it is assumed that the rows are already clean
    """
    cleaned_row = []
    for i in range(len(_row)):
        _x = _row[i]
        field_name = field_names[i]
        target_type = target_fields[field_name.lower()]["type"]
        target_length = target_fields[field_name.lower()]["length"]

        try:
            if target_type == "String":
                if type(_x) is str:
                    _x = unicode(_x.strip(), encoding="utf8")
                    for a in ["!", "@", "#", "$", "%", "^", "&", "*", ","]:
                        _x = _x.replace(a, u"-")
                        _x = _x.strip()
                        if len(_x) > target_length:
                            _x = u""
                elif type(_x) is unicode:
                    for a in ["!", "@", "#", "$", "%", "^", "&", "*", ","]:
                        _x = _x.replace(a, u"-")
                        _x = _x.strip()
                        if len(_x) > target_length:
                            _x = u""
                if not _x:
                    _x = u""
                # else:
                #     try:
                #         _x = _x.encode('ascii', errors='xmlcharrefreplace')
                #     except Exception as e:
                #         pass
            elif target_type == "Date":
                if not _x:
                    _x = None
            elif target_type == "Double":
                if not _x:
                    _x = 0.0
            else:
                if not _x:
                    _x = None

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
        arcpy.AddMessage("UpdateNoiseMitSDE.SdeConnector.create_sde_connection()")
        arcpy.AddMessage("out_folder: {}, connection_name: {}, platform: {}, instance: {}, options: {}".format(
            self.out_folder, self.connection_name, self.platform, self.instance, self.options))
        # delete the sde file if it exists
        loc = os.path.join(self.out_folder, self.connection_name)
        if os.path.exists(loc):
            os.remove(loc)

        try:
            sd = arcpy.CreateDatabaseConnection_management(self.out_folder, self.connection_name, self.platform,
                                                           self.instance,
                                                           **self.options)
            # input path to the dev sde database connection
            target_sde = sd.getOutput(0)
            if not os.path.exists(target_sde):
                raise Exception("The sde file was not created")
            else:
                print("The connection file {} was created".format(target_sde))
                return target_sde

        except Exception as e:
            arcpy.AddError(e.message)


class VersionManager:
    def __init__(self, opt, connection_folder, target_sde, new_version, new_connection, platform, instance):

        self.opt = opt
        self.connection_folder = connection_folder
        self.target_sde = target_sde
        parent_version = opt["version"]
        self.new_version = new_version
        self.new_connection = new_connection
        self.version_sde = ""
        self.parent_version = parent_version
        self.platform = platform
        self.instance = instance
        self.edit_version = ""
        pass

    def clean_previous(self):
        arcpy.AddMessage("UpdateNoiseMitSDE.VersionManager.clean_previous()")
        # remove previous version if it exists
        versions = []
        try:
            if arcpy.Exists(self.target_sde):
                env.workspace = self.target_sde
                versions.extend(da.ListVersions(self.target_sde))
        except EnvironmentError as e:
            raise VersionException(e.message)

        if len(versions):
            new_vers = self.new_version
            v_names = [v.name for v in versions]
            last_names = [x.split(".")[-1] for x in v_names]
            d = dict(list(zip(last_names, v_names)))
            if new_vers in list(d.keys()):
                try:
                    arcpy.DeleteVersion_management(self.target_sde, d[new_vers])
                except Exception as e:
                    arcpy.AddError(e)

        else:
            logging.info("no versions found")

        return True

    def connect_version(self):
        arcpy.AddMessage("UpdateNoiseMitSDE.VersionManager.connect_version()")
        # create version to edit
        versions = da.ListVersions(self.target_sde)

        if len(versions):
            v_names = [v.name for v in versions]
            last_names = [x.split(".")[-1] for x in v_names]
            if self.new_version in last_names:
                raise VersionException("Version already exists, must remove before proceeding")
            else:
                try:
                    arcpy.CreateVersion_management(self.target_sde, self.parent_version, self.new_version,
                                                   access_permission="PUBLIC")
                    # create an sde connection file to the new version
                    v_opt = self.opt.copy()

                    versions = da.ListVersions(self.target_sde)
                    v_names = [v.name for v in versions]
                    last_names = [x.split(".")[-1] for x in v_names]
                    z = list(zip(last_names, v_names))
                    v_store = []
                    for x in z:
                        if self.new_version == x[0]:
                            v_store.append(x[1])

                    self.edit_version = v_store[0]
                    v_opt["version"] = v_store[0]
                    arcpy.AddMessage("Edit Version selected :: {}".format(v_store[0]))

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
        arcpy.AddMessage("UpdateNoiseMitSDE.VersionManager.rec_post()")
        try:
            # Block additional connections during rec/post
            env.workspace = self.version_sde
            logfile = os.path.join(home_dir, "logs\\NoiseMit_logfile.txt")
            if os.path.exists(logfile):
                os.remove(logfile)
            try:
                # putting the version in a list is required for OS Auth versions
                arcpy.ReconcileVersions_management(self.version_sde, "ALL_VERSIONS", u"{}".format(self.parent_version),
                                                   [self.edit_version], "LOCK_ACQUIRED",
                                                   "NO_ABORT", "BY_OBJECT", "FAVOR_TARGET_VERSION",
                                                   "POST", "DELETE_VERSION", logfile)
            except:
                arcpy.DeleteVersion_management(self.version_sde, self.new_version)
                os.remove(self.version_sde)
                raise Exception()

            os.remove(self.version_sde)
            return True
        except Exception as e:
            arcpy.AddError("Unable to rec/post edits :: {}".format(e.message))


class GDBTableUpdater:
    """match_fields, w_table, add_rows, rem_rows, version_sde_file, editor"""
    """weaver_attributes and folioIds are optional arguments, for some of the tools"""
    def __init__(self, match_fields, write_table, read_rows, remove_rows, version_sde,
                 editor, weaver_attributes={}, folioIds=[]):
        self.match_fields = match_fields
        self.write_table = write_table
        self.read_rows = read_rows
        self.remove_rows = remove_rows
        self.version_sde = version_sde
        self.editor = editor
        if weaver_attributes:
            self.folio_field = weaver_attributes["Folio Number"]
        if folioIds:
            self.folioIds = folioIds
        pass

    def insert_rows(self):
        arcpy.AddMessage("UpdateNoiseMitSDE.GDBTableUpdater.insert_rows()")
        try:
            self.editor.startOperation()
            fields = []
            fields.extend(self.match_fields)
            insert = da.InsertCursor(self.write_table, fields)
            i = 0
            for _row in self.read_rows:
                try:
                    insert.insertRow(_row)
                    i += 1
                except Exception as e:
                    arcpy.AddWarning("{} \n {} \n {}".format(e, _row, fields))


            del insert
            if not i:
                arcpy.AddWarning("Rows were not added to the GDB Table")
            else:
                arcpy.AddMessage("{} rows were added to the GDB Table".format(i))
            self.editor.stopOperation()
            return i

        except Exception as h:
            print(h)
            self.editor.stopOperation()
            # raise Exception(h)

    def delete_rows(self):
        arcpy.AddMessage("UpdateNoiseMitSDE.GDBTableUpdater.delete_rows()")
        if "folioIds" in dir(self):
            folio_ids = self.folioIds
            sql_query = "{} in ('{}')".format(self.folio_field, "','".join(folio_ids))
        else:
            sql_query = None

        try:
            self.editor.startOperation()
            i = 0
            rem_rows = self.remove_rows
            with da.UpdateCursor(self.write_table, self.match_fields, sql_query) as _cursor:
                for line in _cursor:
                    # delete all rows that are identical to an item in the input list
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
            print(e)
            self.editor.stopOperation()
            raise Exception(e)

    def update_table(self):
        arcpy.AddMessage("UpdateNoiseMitSDE.GDBTableUpdater.update_table()")
        try:
            # use the update cursor to remove the rem_rows
            deleted, added = 0, 0
            if len(self.remove_rows):
                deleted = self.delete_rows()
            if len(self.read_rows):
                added = self.insert_rows()
            return [deleted, added]

        except Exception as e:
            arcpy.AddError(e)

    def perform_update(self):
        arcpy.AddMessage("UpdateNoiseMitSDE.GDBTableUpdater.perform_update()")
        try:
            # if the table is empty add all read_rows
            if not int(arcpy.GetCount_management(self.write_table).getOutput(0)):
                self.insert_rows()
            else:
                # use the folioIds to filter before updating
                self.update_table()

            self.last_scanned_date()
            return True
        except Exception as e:
            raise Exception(e.message)

    def concatenate(self, join_field, first_field, second_field):
        arcpy.AddMessage("UpdateNoiseMitSDE.GDBTableUpdater.concatenate()")
        d = False
        try:
            self.editor.startOperation()
            with da.UpdateCursor(self.write_table, [join_field, first_field, second_field]) as cursor:
                for row in cursor:
                    if row[1] and row[2]:
                        conc = "{}-{}".format(row[1], row[2])
                        new_row = [conc, row[1], row[2]]
                        cursor.updateRow(new_row)
            d = True
            del cursor
            self.editor.stopOperation()
        except Exception as e:
            logging.error(e)
            d = False
            self.editor.stopOperation()
        return d

    def last_scanned_date(self):
        arcpy.AddMessage("UpdateNoiseMitSDE.GDBTableUpdater.last_scanned_date()")
        try:
            f_lower = [f.name.lower() for f in arcpy.ListFields(self.write_table)]
            if "lastscanneddate" in f_lower:
                self.editor.startOperation()
                # attribute the last scanned date
                field = ["LastScannedDate"]
                with da.UpdateCursor(self.write_table, field) as cursor:
                    for row in cursor:
                        newrow = [datetime.datetime.today()]
                        cursor.updateRow(newrow)
                del cursor
                self.editor.stopOperation()
        except Exception as e:
            arcpy.AddWarning(e)
            self.editor.stopOperation()
            raise Exception(e)


class BuildingsUpdater:
    """buildings, w_table, building_attributes, weaver_attributes, version_sde_file, editor"""

    def __init__(self, domains, folioIds, bldgs, rel_table, bldg_atts, table_atts, combination_atts, version_sde, editor):
        self.domains = domains
        self.folioIds = folioIds
        self.buildings = bldgs
        self.rel_table = rel_table
        self.bldg_folio = bldg_atts["Folio Number"]
        self.bldg_update_fields = bldg_atts
        self.table_folio = table_atts["Folio Number"]
        self.table_update_fields = table_atts
        self.combination_fields = combination_atts
        self.version_sde = version_sde
        self.editor = editor
        self.folios = {}

    def build_folio_dict(self):
        arcpy.AddMessage("UpdateNoiseMitSDE.BuildingUpdater.build_folio_dict()")
        for x in self.folioIds:
            props = dict()
            for fld in self.table_update_fields.keys():
                if fld != "Folio Number":
                    # set the actual field as the key
                    props[self.table_update_fields[fld]] = []
            self.folios[x] = props
        return self.folios

    def concat_list(self, _length, _input):
        """take the input multivalue list and output a string no longer than the max string length parameter"""
        max_length = _length
        _ph = _input
        if type(_ph) == list:
            _ph = list(set(_ph))
            _ph = ", ".join(_ph)

        if len(_ph) > max_length:
            _ph = _ph[:(max_length-4)] + " ..."
        return _ph

    def perform_combination(self, sql_exp1, sql_exp2):
        arcpy.AddMessage("UpdateNoiseMitSDE.BuildingUpdater.perform_combination()")
        # read the source fields and place the value into the target field
        try:
            for x in self.combination_fields:
                source_fields = x["source"]
                # source fields = [folio, lastname, firstname]
                values = dict()
                n = 0
                v = 0
                with da.SearchCursor(self.rel_table, source_fields, sql_exp1) as cursor:
                    for row in cursor:
                        n += 1
                        try:
                            folio = row[0]
                            f1 = row[1]
                            f2 = row[2]
                            if f1 and f2:
                                f1 = f1.capitalize()
                                f2 = f2.capitalize()
                                if f1.upper() != f2.upper():
                                    new_att = u"{}, {}".format(f1, f2)
                                else:
                                    new_att = u"{}".format(f1)
                            else:
                                if f1:
                                    new_att = u"{}".format(f1)
                                elif f2:
                                    new_att = u"{}".format(f2)
                                else:
                                    new_att = u""
                            try:
                                values[folio].append(new_att)
                            except KeyError:
                                values[folio] = [new_att]

                            v += 1
                            del folio, f1, f2, new_att
                        except Exception as e:
                            arcpy.AddWarning(e)
                            
                arcpy.AddMessage("{} rows were scanned from the related table".format(n))
                arcpy.AddMessage("{} names were added to the values dict".format(v))
                arcpy.AddMessage("There are {} folioIds in the values dict".format(len(values.keys())))

                target_fields = x["target"]
                arcpy.AddMessage("target_fields :: {}".format(target_fields))
                arcpy.AddMessage("Buildings = {} , Fields = {}".format(self.buildings, [f.name for f in arcpy.ListFields(self.buildings)]))
                # target_fields = [folioId, contactName]
                self.editor.startOperation()

                num = 0
                i = 0
                n = 0
                error = 0
                keys = values.keys()

                # get the length of each field and shorten text as needed
                field_lengths = {}
                for x in target_fields:
                    fld = arcpy.ListFields(self.buildings, x)[0]
                    if fld.type == "String":
                        field_lengths[x] = fld.length

                with da.UpdateCursor(self.buildings, target_fields, sql_exp2) as cursor:
                    for row in cursor:
                        folio = row[0]
                        num += 1
                        try:
                            if folio in keys:
                                # row[1] is the field length to test
                                max_length = field_lengths[target_fields[1]]
                                contacts = list(set([x for x in values[folio] if x]))
                                contact_str = u"; ".join(contacts)
                                if len(contact_str) > max_length:
                                    contact_str = contact_str[:max_length]
                                new_row = [folio, contact_str]
                                if new_row != row:
                                    try:
                                        cursor.updateRow(new_row)
                                        arcpy.AddMessage("Good Row :: {}".format(new_row))
                                        i += 1
                                    except Exception as e:
                                        arcpy.AddWarning("{} :: {}".format(e, new_row))
                                        error += 1
                                else:
                                    n += 1
                            else:
                                arcpy.AddWarning("Folio {} not found in {}".format(folio, keys))
                        except Exception as e:
                            n += 1
                            arcpy.AddWarning(e)
                        del folio

                arcpy.AddMessage("{} rows were scanned for updating with Contact Name".format(num))
                arcpy.AddMessage("{} buildings were updated with values".format(i))
                arcpy.AddMessage("{} buildings were not updated".format(n))
                arcpy.AddMessage("{} errors occured".format(error))
                self.editor.stopOperation()
                del i, n, error, num
        except RuntimeError as e:
            print(e.message)
            self.editor.stopOperation()
            raise Exception(e)

    def perform_one2one(self, sql_exp2):
        arcpy.AddMessage("UpdateNoiseMitSDE.BuildingUpdater.perform_one2one()")
        # The fields from the source are matched directly to fields in the target
        domain_set = self.domains
        try:
            building_fields = [self.bldg_folio]
            # this adds the actual fields names rather than their label
            building_fields.extend([v for k, v in self.bldg_update_fields.iteritems() if k != "Folio Number"])
            i = 0
            n = 0
            num = 0
            length_lookup = dict()
            for x in building_fields[1:]:
                field = arcpy.ListFields(self.buildings, '*{}'.format(x))[0]
                if field.type == "String":
                    length_lookup[x] = field.length

            self.editor.startOperation()
            with da.UpdateCursor(self.buildings, building_fields, sql_exp2) as _cursor:
                for _row in _cursor:
                    num += 1
                    folio_id = _row[0]
                    table_values = self.folios[folio_id]

                    new_row = [folio_id]
                    _index = 1
                    for x in building_fields[1:]:
                        label = []
                        for k, v in self.bldg_update_fields.iteritems():
                            if v == x:
                                label.append(k)
                                break
                        t_field = []
                        for k, v in self.table_update_fields.iteritems():
                            if k == label[0]:
                                t_field.append(v)
                        try:
                            max_length = length_lookup[x]
                            # get the table values for this field
                            values = table_values[t_field[0]]
                            # create a Counter object on the list of values
                            cnt = Counter(values)

                            if label[0] in domain_set.keys():
                                # retrieve the domain value list
                                value_list = domain_set[label[0]]

                                # get the three most common values from the table_values list
                                most_common = cnt.most_common(3)
                                # starting with the most common value
                                for c in most_common:
                                    item = c[0]
                                    # if it is a string value
                                    if type(item) is str:
                                        # check if it exists in the pick list
                                        if item.lower().strip() in value_list:
                                            # set the final list of values to the single most common string
                                            values = [item]
                                            break
                                        else:
                                            # remove all occurrences of this item from the value list
                                            values = [v for v in values if v != item]
                                    else:
                                        pass
                            new_value = self.concat_list(max_length, values)
                        except KeyError:
                            new_value = _row[_index]

                        new_row.append(new_value)
                        _index += 1

                    if _row != new_row:
                        try:
                            arcpy.AddMessage("This row was updated {}".format(new_row))
                            _cursor.updateRow(new_row)
                            i += 1
                        except Exception as e:
                            print(e)
                    else:
                        # the row has not changed
                        # arcpy.AddMessage("row {} has not changed".format(_row))
                        n += 1
                        pass

            del _cursor
            self.editor.stopOperation()
            arcpy.AddMessage("{} rows were scanned for one-to-one mapping".format(num))
            arcpy.AddMessage("{} buildings were updated with values".format(i))
            arcpy.AddMessage("{} buildings were not updated".format(n))
            del i, n
        except RuntimeError as e:
            print(e.message)
            self.editor.stopOperation()
            raise Exception(e)

    def update_buildings(self):
        arcpy.AddMessage("UpdateNoiseMitSDE.BuildingUpdater.update_buildings()")
        try:
            # gather the building folio numbers as keys in a dict

            self.build_folio_dict()
            # read the rows from the related table with an SQL filter for folioIds;
            # add the field attributes to their list in the _folios dict
            keys = self.folios.keys()
            run = 0

            if len(keys) == 1:
                sql_expression = "{} = '{}'".format(self.table_folio, keys[0])
                run += 1
            elif len(keys) > 1:
                sql_expression = "{} in ('{}')".format(self.table_folio, u"', '".join(keys))
                run += 1
            else:
                arcpy.AddMessage("keys: {}".format(len(keys)))

            if run:
                table_fields = [self.table_folio]
                # this adds the actual fields names rather than their label
                table_fields.extend([v for k, v in self.table_update_fields.iteritems() if k != "Folio Number"])
                with da.SearchCursor(self.rel_table, table_fields, sql_expression) as _cursor:
                    for _row in _cursor:
                        try:
                            for x in self.folios[_row[0]].keys():
                                # these keys are the attribute field names
                                _index = table_fields.index(x)
                                self.folios[_row[0]][x].append(_row[_index])
                        except Exception as e:
                            arcpy.AddWarning(e)

                if len(keys) == 1:
                    sql_expression2 = "{} = '{}'".format(self.bldg_folio, keys[0])
                elif len(keys) > 1:
                    sql_expression2 = "{} in ('{}')".format(self.bldg_folio, "', '".join(keys))

                arcpy.AddMessage("The buildings are now being updated")

                self.perform_one2one(sql_expression2)
                if self.combination_fields:
                    self.perform_combination(sql_exp1=sql_expression, sql_exp2=sql_expression2)

            else:
                arcpy.AddWarning("Buildings were not updated.")

            return True
        except Exception as e:
            arcpy.AddError(e)
