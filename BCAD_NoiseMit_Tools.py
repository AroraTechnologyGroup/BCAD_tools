import os
import sys
import traceback
import datetime
import arcpy
from arcpy import da
from arcpy import env
import json
from utils import UpdateNoiseMitSDE
from utils.UpdateNoiseMitSDE import VersionManager, GDBTableUpdater, BuildingsUpdater

env.overwriteOutput = 1

home_dir = os.path.dirname(os.path.abspath(__file__))

environ = "arora"
version = 'v1.4'

domain_file = os.path.join(home_dir, "utils/domains.json")
file = open(domain_file, 'r')
domains = json.loads(file.read())
file.close()


def get_versioned_fc(workspace, name):
    arcpy.AddMessage("BCAD_NoiseMit_Tools.get_versioned_fc()")
    env.workspace = workspace
    noisemit = arcpy.ListDatasets("*Noise*")[0]
    dataset_path = os.path.join(env.workspace, noisemit)
    env.workspace = dataset_path
    fcs = arcpy.ListFeatureClasses("*{}".format(name))
    if len(fcs) == 1:
        fc = fcs.pop()
        if arcpy.Exists(fc):
            fc_path = os.path.join(dataset_path, fc)
            return fc_path


def execute_tool(tool, params):
    arcpy.AddMessage("BCAD_NoiseMit_Tools.execute_tool()")
    connection_folder = params["connection_folder"]
    platform = params["platform"]
    instance = params["instance"]
    sde_file = params["gis_gdb"]

    sql_table = params["sql_table"]
    gdb_table = params["gdb_table"]
    gdb_table_name = params["gdb_table_name"]

    edit_connection_name = params["edit_connection_name"]
    edit_version_name = params["edit_version_name"]

    opt = params["opt"]

    keys = params.keys()
    # check if an external database is holding the source tables
    table_db = None
    if "table_db" in keys:
        table_db = params["table_db"]

    # check if domains are passed in params
    domain_set = None
    if "domains" in keys:
        domain_set = params["domains"]

    # check if buildings are in params
    buildings_name = None
    building_attributes = None
    table_attributes = None
    bldgs = None
    if "bldgs" in keys:
        bldgs = params["bldgs"]
    if "buildings_name" in keys:
        buildings_name = params["buildings_name"]
    if "building_attributes" in keys:
        building_attributes = params["building_attributes"]
    if "table_attributes" in keys:
        table_attributes = params["table_attributes"]

    # check if an agreement and leasehold fields are params
    agreement_field = None
    leasehold_field = None
    if "agreement_field" in keys:
        agreement_field = params["agreement_field"]
    if "leasehold_field" in keys:
        leasehold_field = params["leasehold_field"]

    # check if fields are being combined during the update
    combination_attributes = None
    join_field = None
    if "combination_attributes" in keys:
        combination_attributes = params["combination_attributes"]
    if "join_field" in keys:
        join_field = params["join_field"]

    try:
        # Fail the Tool if the Source tables are empty
        i = 0
        with arcpy.da.SearchCursor(sql_table, "*") as cursor:
            for row in cursor:
                i += 1
                break
        if not i:
            raise Exception("No rows exist in the source table {}".format(sql_table))

        try:

            result = UpdateNoiseMitSDE.compare_tables(sql_table=sql_table, gdb_table=gdb_table)

            compare_result = result["compare_result"]
            folioIds = result["folioIds"]
            match_fields = result["match_fields"]
            add_rows = result["add_rows"]
            exist_rows = result["exist_rows"]

            # create VersionManager class object to create new version, connect to it,
            # and create an sde connection file, set as current workspace

            version_manager = VersionManager(opt, connection_folder, sde_file, edit_version_name, edit_connection_name,
                                             platform, instance)
            version_manager.clean_previous()
            version_sde_file = version_manager.connect_version()

            if os.path.exists(version_sde_file):
                arcpy.AddMessage(version_sde_file)
            else:
                raise Exception("version_sde_file not created")

            editor = da.Editor(version_sde_file)
            editor.startEditing()
            # ensure that editing is stopped following an exception
            try:
                env.workspace = version_sde_file
                gdb_table = arcpy.ListTables("*{}*".format(gdb_table_name))[0]
                gdb_table = os.path.join(env.workspace, gdb_table)
                print(dir(GDBTableUpdater))
                """
                match_fields, write_table, read_rows, remove_rows, version_sde,
                editor, weaver_attributes = {}, folioIds = []
                """

                table_updater = GDBTableUpdater(match_fields, gdb_table, add_rows, exist_rows,
                                                version_sde_file, editor, table_attributes, folioIds)
                # compare result, if True, means that changes need to be made to the GDB Table
                if compare_result:
                    arcpy.AddMessage({"# rows to add": len(add_rows),
                                      "# rows to remove": len(exist_rows)})

                    if arcpy.Exists(gdb_table):
                        # should return True when editing is complete
                        try:
                            table_updater.perform_update()
                            # if the join field to calculate from two input field is specified perform the concatenation
                            if join_field:
                                table_updater.concatenate(join_field, agreement_field, leasehold_field)

                            # If the feature class to edit has been specified; create BuildingUpdater class object
                            if buildings_name:
                                version_buildings = tool.get_versioned_fc(version_sde_file, buildings_name)
                                if arcpy.Exists(version_buildings):
                                    try:
                                        print(dir(BuildingsUpdater))
                                        building_updater = BuildingsUpdater(domain_set, folioIds, version_buildings, gdb_table, building_attributes,
                                                                            table_attributes, combination_attributes, version_sde_file, editor)

                                        building_updater.update_buildings()
                                        editor.stopEditing(True)
                                    except Exception as e:
                                        raise Exception("Exception occured during buildings updates, edits have not been saved :: {}" \
                                                       ":: {}".format(e, traceback.print_exc()))
                                else:
                                    raise Exception("Unable to locate the buildings feature class")
                            else:
                                arcpy.AddMessage("Building feature class name not specified, so only a table is being updated")
                                table_updater.last_scanned_date()
                                editor.stopEditing(True)
                                del editor
                        except:
                            raise Exception("Error during GDB Table update")
                    else:
                        raise Exception("Unable to determine the gdb table using the version connection")
                else:
                    arcpy.AddMessage("The files are identical, apply the last scanned date")
                    # This is important to add the datetime that the script was last run
                    table_updater.last_scanned_date()
                    editor.stopEditing(True)
                    del editor

                try:
                    version_manager.rec_post()
                except Exception as e:
                    arcpy.AddError("Exception occurred during the rec/post operation, " +
                                   "the edits were saved in the version however the version will be removed without the " +
                                   "edits having been posted to the default version :: {} :: {}".format(e, traceback.print_exc()))
                try:
                    version_manager.clean_previous()
                    del version_manager
                except:
                    arcpy.AddError("Changed were saved and posted.  However, the edit version was not removed")

                # Verify that the edits where posted
                # TODO- determine failproof methods for isolating the changed features and viewing the change
                if building_attributes:
                    env.workspace = sde_file
                    fields = [x for x in list(building_attributes.values())]
                    if combination_attributes:
                        for x in combination_attributes:
                            fs = x["target"]
                            fields.append(fs[1])

                    cursor = da.SearchCursor(bldgs, fields,
                                             "{} in ('{}')".format(building_attributes["Folio Number"], "','".join(folioIds)))
                    try:
                        values = cursor.next()
                        arcpy.AddMessage("This is an edited row in the buildings table :: {}".format(values))
                    except StopIteration:
                        arcpy.AddMessage("No buildings found with folioIDs in {}".format(folioIds))
                    del cursor
                return True

            except Exception as e:
                if editor:
                    editor.stopEditing(False)
                    del editor
                version_manager.clean_previous()
                raise Exception("Edits were not saved, the NoiseMit Version has been removed :: {}".format(e))
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            arcpy.AddError(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
    except Exception as e:
        arcpy.AddError(e)


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Toolbox"
        self.alias = "Noise Mit Tools {}".format(version)

        # List of tool classes associated with this toolbox
        self.tools = [WeaverGDBUpdate, CARsGDBUpdate, LeaseUpdate]


class WeaverGDBUpdate(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "WeaverGDBUpdate"
        self.description = "The Weaver export to a SQL Table is used to update the GDB table " + \
                           "that particapates in a one-to-many relationship class with the noise buildings feature class"
        self.canRunInBackground = False
        self.get_versioned_fc = get_versioned_fc

    def getParameterInfo(self):
        """Define parameter definitions"""

        param0 = arcpy.Parameter(
            displayName='GISGDB Workspace',
            name='gis_geodatabase',
            datatype='DEWorkspace',
            parameterType='Required',
            direction='Input',
        )
        param0.filter.list = ["Remote Database"]

        if environ == "bcad":
            param0.value = os.path.join(home_dir, 'DBConnections\\ad_gisair_dev.sde')
        elif environ == "arora":
            param0.value = os.path.join(home_dir, 'DBConnections\\BCAD_Test.sde')

        param01 = arcpy.Parameter(
            displayName='Table Storage Database',
            name='storage_db',
            datatype='DEWorkspace',
            parameterType='Required',
            direction='Input'
        )
        param01.filter.list = ["Remote Database"]

        if environ == "bcad":
            param01.value = os.path.join(home_dir, 'DBConnections\\ad_noisemit.sde')
        elif environ == "arora":
            param01.value = os.path.join(home_dir, 'DBConnections\\TestTables.sde')

        param02 = arcpy.Parameter(
            displayName='Operating System Authentication',
            name='os_authentication',
            datatype='GPBoolean',
            parameterType='Optional',
            direction='Input'
        )

        if environ == "bcad":
            param02.value = True
        elif environ == "arora":
            param02.value = True

        # # username for the database user
        param03 = arcpy.Parameter(
            displayName='Database Username',
            name='database_username',
            datatype='GPString',
            parameterType='Optional',
            direction='Input'
        )

        if environ == "bcad":
            param03.value = ''
        elif environ == "arora":
            param03.value = ''

        # password for the database user
        param04 = arcpy.Parameter(
            displayName='Database User Password',
            name='password',
            datatype='GPString',
            parameterType='Optional',
            direction='Input'
        )

        if environ == "bcad":
            param04.value = ''
        elif environ == "arora":
            param04.value = ''

        # variable for the parent version of the database
        param05 = arcpy.Parameter(
            displayName='Default Version',
            name='default_version',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param05.value = 'sde.DEFAULT'
        elif environ == "arora":
            param05.value = 'dbo.DEFAULT'

        # name of building polygon feature class
        param06 = arcpy.Parameter(
            displayName='Building Information Feature Class',
            name='buildings',
            datatype='DEFeatureClass',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param06.value = '{}\\GISAIRD.BCAD.Noise_Mitigation\\GISAIRD.BCAD.NoiseBuilding'.format(param0.value)
        elif environ == "arora":
            param06.value = '{}\\bcad_noise.DBO.Noise_Mitigation\\bcad_noise.DBO.NoiseBuilding'.format(param0.value)

        # sql table used to update the geodatabase table
        param07 = arcpy.Parameter(
            displayName='Weaver SQL Table',
            name='sql_table',
            datatype='DETable',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param07.value = '{}\\NoiseMit.bcad.WeaverDataImport'.format(param01.value)
        elif environ == "arora":
            param07.value = '{}\\bcad_noisemit_tables.DBO.WeaverDataImport'.format(param01.value)

        # GDB table with holds the weaver data from the sql table
        param08 = arcpy.Parameter(
            displayName='Relationship GDB Table',
            name='gdb_table',
            datatype='DETable',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param08.value = r'{}\\GISAIRD.BCAD.WeaverDataImport'.format(param0.value)
        elif environ == "arora":
            param08.value = r'{}\\bcad_noise.DBO.WeaverDataImport'.format(param0.value)

        param09 = arcpy.Parameter(
            displayName='Buildings ProjectName Field',
            name='bldgs_project_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param09.parameterDependencies = [param06.name]
        param09.filter.list = ['Text']
        param09.value = "projectName"

        param10 = arcpy.Parameter(
            displayName='Buildings PhaseName Field',
            name='bldgs_phase_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param10.parameterDependencies = [param06.name]
        param10.filter.list = ['Text']
        param10.value = "phaseName"

        param11 = arcpy.Parameter(
            displayName='Buildings FolioID Field',
            name='bldgs_folioId_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param11.parameterDependencies = [param06.name]
        param11.filter.list = ['Text']
        param11.value = "folioId"

        param12 = arcpy.Parameter(
            displayName='Buildings Contact Name Field',
            name='bldgs_contact_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param12.parameterDependencies = [param06.name]
        param12.filter.list = ['Text']
        param12.value = "SSACARPropContact"

        param13 = arcpy.Parameter(
            displayName='GDB Table ProjectName Field',
            name='gdb_table_project_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param13.parameterDependencies = [param08.name]
        param13.filter.list = ['Text']
        # It was requested that the PhaseStatus field from the table be used to populate
        #  the ProjectName field on the buildings
        param13.value = "PhaseStatus"

        param14 = arcpy.Parameter(
            displayName='GDB Table PhaseName Field',
            name='gdb_table_phase_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param14.parameterDependencies = [param08.name]
        param14.filter.list = ['Text']
        param14.value = "PhaseName"

        param15 = arcpy.Parameter(
            displayName='GDB Table FolioNumber Field',
            name='gdb_table_folioNumber_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param15.parameterDependencies = [param08.name]
        param15.filter.list = ['Text']
        param15.value = "FolioNumber"

        param16 = arcpy.Parameter(
            displayName='GDB Table First Name Field',
            name='gdb_table_firstName',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param16.parameterDependencies = [param08.name]
        param16.filter.list = ["Text"]
        param16.value = "FirstName"

        param17 = arcpy.Parameter(
            displayName='GDB Table Last Name Field',
            name='gdb_table_lastName',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param17.parameterDependencies = [param08.name]
        param17.filter.list = ["Text"]
        param17.value = "LastName"

        param18 = arcpy.Parameter(
            displayName="Server Instance",
            name='server_instance',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param18.value = 'fllgissql01'
        elif environ == "arora":
            param18.value = r"ARORALAPTOP50\SDESQLEXPRESS"

        param19 = arcpy.Parameter(
            displayName="Database Name",
            name='database_name',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param19.value = "GISAIRD"
        elif environ == "arora":
            param19.value = "bcad_noise"

        params = [param0, param01,
                  param02, param03,
                  param04, param05, param06,
                  param07, param08, param09, param10, param11, param12, param13,
                  param14, param15, param16, param17, param18, param19]

        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def processParameters(self, parameters):
        arcpy.AddMessage("WeaverGDBUpdate.process_parameters()")

        params = [param.valueAsText for param in parameters]

        # These are the parameters defined by the user
        gis_gdb, table_db, os_auth, uid, pwd, p_version, bldgs,\
        sql_table, gdb_table, bldg_projectName, bldg_phaseName,\
        bldg_folioId, bldg_contactName, gdb_table_projectName, gdb_table_phaseName, \
        gdb_table_folioId, gdb_table_firstName, gdb_table_lastName, server_instance,\
        database = params

        if gdb_table:
            gdb_table_name = gdb_table.split('\\')[-1].split(".")[-1]
        else:
            gdb_table_name = "WeaverDataImport"
        if bldgs:
            buildings_name = bldgs.split('\\')[-1].split(".")[-1]
        else:
            buildings_name = "NoiseBuilding"

        connection_folder = os.path.join(home_dir, "DBConnections")
        # name of the version_sde_file to be created for editing
        edit_connection_name = "NoiseMit.sde"
        # name of the version to be created for editing
        edit_version_name = "NoiseMit"

        plat = r"SQL_SERVER"
        instance = server_instance
        opt = {
            "database": database,
            "version_type": "TRANSACTIONAL",
            "version": p_version,
            "date": "",
            "schema": "#"
        }

        if os_auth == 'true':
            opt["account_authentication"] = "OPERATING_SYSTEM_AUTH"
        else:
            opt["account_authentication"] = "DATABASE_AUTH"
            opt["username"] = uid
            opt["password"] = pwd
            opt["save_user_pass"] = "SAVE_USERNAME"

        # attribute fields on the building feature class that match one-to-one with the field in the table
        building_attributes = {"Project Name": bldg_projectName,
                               "Phase Name": bldg_phaseName,
                               "Folio Number": bldg_folioId}

        # attribute fields on the weaver geodatabase table that match one-to-one with the field in the building
        table_attributes = {"Project Name": gdb_table_projectName,
                            "Phase Name": gdb_table_phaseName,
                            "Folio Number": gdb_table_folioId}

        # the first field in the combination attributes is the ID to match the features
        # Use this object to handle mapping fields with different names
        combination_attributes = [
            {
                "target": [bldg_folioId, bldg_contactName],
                "source": [gdb_table_folioId, gdb_table_lastName, gdb_table_firstName]
            }
        ]

        # These variables are derived from the user parameters, but should be defined in this function
        # to allow for unittests.

        table_domains = domains["WeaverDataImport"]

        final_parameters = {
            "connection_folder": connection_folder,
            "platform": plat,
            "instance": instance,
            "gis_gdb": gis_gdb,
            "table_db": table_db,
            "bldgs": bldgs,
            "sql_table": sql_table,
            "gdb_table": gdb_table,
            "gdb_table_name": gdb_table_name,
            "buildings_name": buildings_name,
            "edit_connection_name": edit_connection_name,
            "edit_version_name": edit_version_name,
            "building_attributes": building_attributes,
            "table_attributes": table_attributes,
            "combination_attributes": combination_attributes,
            "opt": opt,
            "domains": table_domains
        }

        return final_parameters

    def execute(self, parameters, messages):
        """The method calls classes defined in external files."""
        arcpy.AddMessage("WeaverGDBUpdate.execute()")
        params = self.processParameters(parameters=parameters)
        success = execute_tool(self, params)
        return success


class CARsGDBUpdate(object):
    def __init__(self):
        self.label = "CARsGDBUpdate"
        self.description = "The CARs export to a SQL Table is used to update the GDB table " + \
                           "that participates in a one-to-many relationship class with the SSACARBuildings feature class"
        self.canRunInBackground = False
        self.get_versioned_fc = get_versioned_fc

    def getParameterInfo(self):
        param0 = arcpy.Parameter(
            displayName='GISGDB Workspace',
            name='gis_geodatabase',
            datatype='DEWorkspace',
            parameterType='Required',
            direction='Input',
        )
        param0.filter.list = ["Remote Database"]

        if environ == "bcad":
            param0.value = os.path.join(home_dir, 'DBConnections\\ad_gisair_dev.sde')
        elif environ == "arora":
            param0.value = os.path.join(home_dir, 'DBConnections\\BCAD_Test.sde')

        param01 = arcpy.Parameter(
            displayName='Table Storage Database',
            name='storage_db',
            datatype='DEWorkspace',
            parameterType='Required',
            direction='Input'
        )
        param01.filter.list = ["Remote Database"]

        if environ == "bcad":
            param01.value = os.path.join(home_dir, 'DBConnections\\ad_noisemit.sde')
        elif environ == "arora":
            param01.value = os.path.join(home_dir, 'DBConnections\\TestTables.sde')

        param02 = arcpy.Parameter(
            displayName='Operating System Authentication',
            name='os_authentication',
            datatype='GPBoolean',
            parameterType='Optional',
            direction='Input'
        )

        if environ == "bcad":
            param02.value = True
        elif environ == "arora":
            param02.value = True

        # # username for the database user
        param03 = arcpy.Parameter(
            displayName='Database Username',
            name='database_username',
            datatype='GPString',
            parameterType='Optional',
            direction='Input'
        )

        if environ == "bcad":
            param03.value = ''
        elif environ == "arora":
            param03.value = ''

        # password for the database user
        param04 = arcpy.Parameter(
            displayName='Database User Password',
            name='password',
            datatype='GPString',
            parameterType='Optional',
            direction='Input'
        )

        if environ == "bcad":
            param04.value = ''
        elif environ == "arora":
            param04.value = ''

        # variable for the parent version of the database
        param05 = arcpy.Parameter(
            displayName='Default Version',
            name='default_version',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param05.value = 'sde.DEFAULT'
        elif environ == "arora":
            param05.value = 'dbo.DEFAULT'

        # name of building polygon feature class
        param06 = arcpy.Parameter(
            displayName='SSACAR Building Feature Class',
            name='ssa_car_buildings',
            datatype='DEFeatureClass',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param06.value = r'{}\GISAIRD.BCAD.Noise_Mitigation\GISAIRD.BCAD.SSACARBuilding'.format(param0.value)
        elif environ == "arora":
            param06.value = r'{}\bcad_noise.DBO.Noise_Mitigation\bcad_noise.DBO.SSACARBuilding'.format(param0.value)

        # sql table used to update the geodatabase table
        param07 = arcpy.Parameter(
            displayName='SSACAR SQL Table',
            name='ssa_car_sql_table',
            datatype='DETable',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param07.value = r'{}\NoiseMit.dbo.WeaverProgramStatus'.format(param01.value)
        elif environ == "arora":
            param07.value = r'{}\bcad_noisemit_tables.DBO.WEAVERPROGRAMSTATUS'.format(param01.value)

        # GDB table which holds the weaver data from the sql table
        param08 = arcpy.Parameter(
            displayName='Relationship GDB Table',
            name='gdb_table',
            datatype='DETable',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param08.value = r'{}\GISAIRD.BCAD.SSACAR'.format(param0.value)
        elif environ == "arora":
            param08.value = r'{}\bcad_noise.DBO.SSACAR'.format(param0.value)

        param09 = arcpy.Parameter(
            displayName='Property Contact Name',
            name='prop_contact_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param09.parameterDependencies = [param06.name]
        param09.filter.list = ['Text']
        param09.value = "SSACARPropContact"

        param10 = arcpy.Parameter(
            displayName='SSACAR Phase',
            name='ssa_car_phase',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param10.parameterDependencies = [param06.name]
        param10.filter.list = ['Text']
        param10.value = "SSACARPHASE"

        param11 = arcpy.Parameter(
            displayName='SSACAR Status',
            name='ssa_car_status',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param11.parameterDependencies = [param06.name]
        param11.filter.list = ['Text']
        param11.value = "SSACARSTATUS"

        param12 = arcpy.Parameter(
            displayName='SSACAR Buildings FolioID Field',
            name='ssacar_bldgs_folioId',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param12.parameterDependencies = [param06.name]
        param12.filter.list = ['Text']
        param12.value = "folioId"

        param13 = arcpy.Parameter(
            displayName='SSACAR GDB Table Contact Field',
            name='ssacar_gdb_table_contact',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param13.parameterDependencies = [param08.name]
        param13.filter.list = ['Text']
        param13.value = "ContactName"

        param14 = arcpy.Parameter(
            displayName='SSACAR GDB Table WaitList/Phase Field',
            name='ssacar_gdb_table_phase',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param14.parameterDependencies = [param08.name]
        param14.filter.list = ['Text']
        param14.value = "WaitListName"

        param15 = arcpy.Parameter(
            displayName='SSACAR GDB Table WaitList/Status Field',
            name='ssacar_gdb_table_status',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param15.parameterDependencies = [param08.name]
        param15.filter.list = ['Text']
        param15.value = "WaitListStatusTypeName"

        param16 = arcpy.Parameter(
            displayName='SSACAR GDB Table FolioNumber Field',
            name='gdb_table_folioNumber_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param16.parameterDependencies = [param08.name]
        param16.filter.list = ['Text']
        param16.value = "FolioNumber"

        param17 = arcpy.Parameter(
            displayName="Server Instance",
            name='server_instance',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param17.value = 'fllgissql01'
        elif environ == "arora":
            param17.value = r"ARORALAPTOP50\SDESQLEXPRESS"

        param18 = arcpy.Parameter(
            displayName="Database Name",
            name='database_name',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param18.value = "GISAIRD"
        elif environ == "arora":
            param18.value = "bcad_noise"


        params = [param0, param01,
                  param02, param03,
                  param04, param05, param06,
                  param07, param08, param09, param10, param11, param12, param13,
                  param14, param15, param16, param17, param18]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def processParameters(self, parameters):
        arcpy.AddMessage("BCAD_NoiseMit_Tools.CARsGDBUpdate.process_parameters()")

        params = [p.valueAsText for p in parameters]
        # These are the parameters defined by the user
        gis_gdb, table_db, os_auth, uid, pwd, p_version, bldgs, \
        sql_table, gdb_table, bldg_contactName, bldg_phaseName, bldg_status, \
        bldg_folioId, gdb_table_contactName, gdb_table_phaseName, gdb_table_status, \
        gdb_table_folioId, server_instance, database = params

        if gdb_table:
            gdb_table_name = gdb_table.split("\\")[-1].split(".")[-1]
        else:
            gdb_table_name = "SSACAR"
        if bldgs:
            buildings_name = bldgs.split('\\')[-1].split(".")[-1]
        else:
            buildings_name = "SSACARBuilding"

        connection_folder = os.path.join(home_dir, "DBConnections")
        # name of the version_sde_file to be created for editing
        edit_connection_name = "NoiseMit.sde"
        # name of the version to be created for editing
        edit_version_name = "NoiseMit"

        plat = r"SQL_SERVER"
        instance = server_instance
        opt = {
            "database": database,
            "version_type": "TRANSACTIONAL",
            "version": p_version,
            "date": "",
            "schema": "#"
        }

        if os_auth == 'true':
            opt["account_authentication"] = "OPERATING_SYSTEM_AUTH"
        else:
            opt["account_authentication"] = "DATABASE_AUTH"
            opt["username"] = uid
            opt["password"] = pwd
            opt["save_user_pass"] = "SAVE_USERNAME"

        # attribute fields on the building feature class that will be updated
        building_attributes = {"Contact Name": bldg_contactName,
                               "Phase Name": bldg_phaseName,
                               "Status": bldg_status,
                               "Folio Number": bldg_folioId}
        # attribute fields on the geodatabase table that are source data
        table_attributes = {"Contact Name": gdb_table_contactName,
                            "Phase Name": gdb_table_phaseName,
                            "Status": gdb_table_status,
                            "Folio Number": gdb_table_folioId}
        # fields that are being combined should be listed here
        # combination_attributes = [
        #     {
        #     "target": "",
        #     "source": []
        #     }
        # ]
        # These variables are derived from the user parameters, but should be set in this function
        # to allow for importing into the unittests directly.

        table_domains = domains["SSACAR"]

        final_parameters = {
            "connection_folder": connection_folder,
            "platform": plat,
            "instance": instance,
            "gis_gdb": gis_gdb,
            "table_db": table_db,
            "bldgs": bldgs,
            "sql_table": sql_table,
            "gdb_table": gdb_table,
            "gdb_table_name": gdb_table_name,
            "buildings_name": buildings_name,
            "edit_connection_name": edit_connection_name,
            "edit_version_name": edit_version_name,
            "building_attributes": building_attributes,
            "table_attributes": table_attributes,
            "opt": opt,
            "domains": table_domains
        }

        return final_parameters

    def execute(self, parameters, message):
        """The method calls classes defined in external files."""
        arcpy.AddMessage("BCAD_NoiseMit_Tools.CARsGDBUpdate.execute()")
        params = self.processParameters(parameters=parameters)
        success = execute_tool(self, params)
        return success


class LeaseUpdate(object):
    def __init__(self):
        self.label = "LeaseUpdate"
        self.description = "A View of the Propworks SQL Table is being used to update a Lease GDB Table"
        self.canRunInBackground = False

    def getParameterInfo(self):
        param0 = arcpy.Parameter(
            displayName='GISGDB Workspace',
            name='gis_geodatabase',
            datatype='DEWorkspace',
            parameterType='Required',
            direction='Input',
        )
        param0.filter.list = ["Remote Database"]

        if environ == "bcad":
            param0.value = os.path.join(home_dir, 'DBConnections\\ad_gisair_dev.sde')
        elif environ == "arora":
            param0.value = os.path.join(home_dir, 'DBConnections\\BCAD_Test.sde')

        param01 = arcpy.Parameter(
            displayName='Staging Table Workspace',
            name='table_geodatabase',
            datatype='DEWorkspace',
            parameterType='Required',
            direction='Input',
        )
        param01.filter.list = ["Remote Database"]
        if environ == "bcad":
            param01.value = param0.value
        elif environ == "arora":
            param01.value = os.path.join(home_dir, 'DBConnections\\TestTables.sde')

        param02 = arcpy.Parameter(
            displayName='Operating System Authentication',
            name='os_authentication',
            datatype='GPBoolean',
            parameterType='Optional',
            direction='Input'
        )

        if environ == "bcad":
            param02.value = True
        elif environ == "arora":
            param02.value = True

        # # username for the database user
        param03 = arcpy.Parameter(
            displayName='Database Username',
            name='database_username',
            datatype='GPString',
            parameterType='Optional',
            direction='Input'
        )

        if environ == "bcad":
            param03.value = ''
        elif environ == "arora":
            param03.value = ''

        # password for the database user
        param04 = arcpy.Parameter(
            displayName='Database User Password',
            name='password',
            datatype='GPString',
            parameterType='Optional',
            direction='Input'
        )

        if environ == "bcad":
            param04.value = ''
        elif environ == "arora":
            param04.value = ''

        # variable for the parent version of the database
        param05 = arcpy.Parameter(
            displayName='Default Version',
            name='default_version',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param05.value = 'sde.DEFAULT'
        elif environ == "arora":
            param05.value = 'dbo.DEFAULT'

        # sql table used to update the geodatabase table
        param06 = arcpy.Parameter(
            displayName='DBO.Lease Table / View of Propworks',
            name='dbo.lease_table',
            datatype='DETable',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param06.value = r'{}\GISAIRD.dbo.leases'.format(param01.value)
        elif environ == "arora":
            param06.value = r'{}\bcad_noisemit_tables.DBO.leases'.format(param01.value)

        # GDB table which holds the weaver data from the sql table
        param07 = arcpy.Parameter(
            displayName='Relationship GDB Table',
            name='gdb_table',
            datatype='DETable',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param07.value = r'{}\GISAIRD.BCAD.Leases'.format(param0.value)
        elif environ == "arora":
            param07.value = r'{}\bcad_noise.DBO.Leases'.format(param0.value)

        param08 = arcpy.Parameter(
            displayName='Agreement-Leasehold Field',
            name='agreement_lease_field',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )

        param08.filter.list = ['Text']
        param08.parameterDependencies = [param07.name]

        if environ == 'bcad':
            param08.value = "AGREEMENT_LEASE"
        elif environ == "arora":
            param08.value = "AGREEMENT_LEASE"

        param09 = arcpy.Parameter(
            displayName='Agreement Field',
            name='agreement_field',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )

        param09.filter.list = ['Text']
        param09.parameterDependencies = [param07.name]

        if environ == 'bcad':
            param09.value = "AGREEMENT_NUMBER"
        elif environ == "arora":
            param09.value = "AGREEMENT_NUMBER"

        param10 = arcpy.Parameter(
            displayName='Leasehold Field',
            name='leasehold_field',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )

        param10.filter.list = ['Text']
        param10.parameterDependencies = [param07.name]

        if environ == 'bcad':
            param10.value = "LEASEHOLD_NUMBER"
        elif environ == "arora":
            param10.value = "LEASEHOLD_NUMBER"

        param11 = arcpy.Parameter(
            displayName="Server Instance",
            name='server_instance',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param11.value = 'fllgissql01'
        elif environ == "arora":
            param11.value = r"ARORALAPTOP50\SDESQLEXPRESS"

        param12 = arcpy.Parameter(
            displayName="Database Name",
            name='database_name',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )

        if environ == "bcad":
            param12.value = "GISAIRD"
        elif environ == "arora":
            param12.value = "bcad_noise"

        params = [param0, param01, param02, param03, param04, param05, param06, param07, param08, param09, param10, param11, param12]
        return params

    def processParameters(self, parameters):
        arcpy.AddMessage("BCAD_NoiseMit_Tools.LeaseUpdate.process_parameters()")
        params = [p.valueAsText for p in parameters]

        gis_gdb, table_gdb, os_auth, uid, pwd, p_version,  \
        view_table, gdb_table, join_field, agreement_field, leasehold_field, server_instance, database = params

        if gdb_table:
            gdb_table_name = gdb_table.split("\\")[-1].split(".")[-1]
        else:
            gdb_table_name = "Leases"

        connection_folder = os.path.join(home_dir, "DBConnections")
        plat = r"SQL_SERVER"
        instance = server_instance
        opt = {
            "database": database,
            "version_type": "TRANSACTIONAL",
            "version": p_version,
            "date": "",
            "schema": "#"
        }

        if os_auth == 'true':
            opt["account_authentication"] = "OPERATING_SYSTEM_AUTH"
        else:
            opt["account_authentication"] = "DATABASE_AUTH"
            opt["username"] = uid
            opt["password"] = pwd
            opt["save_user_pass"] = "SAVE_USERNAME"

        # name of the version_sde_file to be created for editing
        edit_connection_name = "Leases.sde"
        # name of the version to be created for editing
        edit_version_name = "Leases"

        final_parameters = {
            "connection_folder": connection_folder,
            "platform": plat,
            "instance": instance,
            "gis_gdb": gis_gdb,
            "join_field": join_field,
            "agreement_field": agreement_field,
            "leasehold_field": leasehold_field,
            "sql_table": view_table,
            "gdb_table": gdb_table,
            "gdb_table_name": gdb_table_name,
            "edit_connection_name": edit_connection_name,
            "edit_version_name": edit_version_name,
            "opt": opt,
        }

        return final_parameters

    def execute(self, parameters, message):
        """The method calls classes defined in external files."""
        arcpy.AddMessage("BCAD_NoiseMit_Tools.LeaseUpdate.execute()")
        params = self.processParameters(parameters)
        # the execute_tool function updates the GDB and optionally updates specified attributes on a features class,
        # for this tool, we need to perform a concatenation of two fields into the Agreement_Lease field
        # this field parameter is removed from params
        success = execute_tool(self, params)
        return success
