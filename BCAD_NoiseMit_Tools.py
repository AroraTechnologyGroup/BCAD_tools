import arcpy
import sys
import os
from arcpy import da
from arcpy import env
import traceback
import UpdateNoiseMitSDE as Tool
from UpdateNoiseMitSDE import SdeConnector, VersionManager, GDBTableUpdater, BuildingsUpdater
env.overwriteOutput = 1


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Toolbox"
        self.alias = "Noise Mit Tools"

        # List of tool classes associated with this toolbox
        self.tools = [WeaverGDBUpdate]


class WeaverGDBUpdate(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "WeaverGDBUpdate to GDB table"
        self.description = "The Weaver export to a SQL Table is used to update the GDB table " + \
            "that particapates in a one-to-many relationship class with the buildings feature class"
        self.canRunInBackground = True

    def getParameterInfo(self):
        """Define parameter definitions"""
        # folder to store the connection files created in the script
        param0 = arcpy.Parameter(
            displayName='Temp Folder',
            name='temp_folders',
            datatype='DEFolder',
            parameterType='Required',
            direction='Input'
        )
        param0.defaultEnvironmentName = 'scratchFolder'
        param0.value = r"G:\Users\rhughes\AppData\Roaming\ESRI\Desktop10.4\ArcCatalog"

        # name of the database instance
        param01 = arcpy.Parameter(
            displayName='SQL Server Instance',
            name='sql_server_instance',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )
        param01.value = r"sql-server-azure.database.windows.net"

        # username for the database user
        param02 = arcpy.Parameter(
            displayName='Database Username',
            name='database_username',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )
        param02.value = 'bcad'

        # password for the database user
        param03 = arcpy.Parameter(
            displayName='Database User Password',
            name='password',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )
        param03.value = r'AroraGIS123'

        # name of the database
        param04 = arcpy.Parameter(
            displayName='Database Name',
            name='database_name',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )
        param04.value = "bcad_noise"

        # variable for the parent version of the database
        param05 = arcpy.Parameter(
            displayName='Default Version',
            name='default_version',
            datatype='GPString',
            parameterType='Required',
            direction='Input'
        )
        param05.value = 'dbo.DEFAULT'

        # name of building polygon feature class
        param06 = arcpy.Parameter(
            displayName='Building Information Feature Class',
            name='buildings',
            datatype='DEFeatureClass',
            parameterType='Required',
            direction='Input'
        )
        param06.value = r"Database Connections\bcad_noise.sde\bcad_noise.DBO.NoiseMitigation\bcad_noise.DBO.Building_Information"

        # sql table used to update the geodatabase table
        param07 = arcpy.Parameter(
            displayName='Weaver SQL Table',
            name='sql_table',
            datatype='DETable',
            parameterType='Required',
            direction='Input'
        )
        param07.value = r"Database Connections\bcad_noise.sde\bcad_noise.DBO.weaver_formatted"

        # GDB table with holds the weaver data from the sql table
        param08 = arcpy.Parameter(
            displayName='Relationship GDB Table',
            name='gdb_table',
            datatype='DETable',
            parameterType='Required',
            direction='Input'
        )
        param08.value = r"Database Connections\bcad_noise.sde\bcad_noise.DBO.WEAVER"

        param09 = arcpy.Parameter(
            displayName='Buildings ProjectName Field',
            name='bldgs_project_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param09.parameterDependencies = [param06.name]
        param09.value = "projectName"

        param10 = arcpy.Parameter(
            displayName='Buildings PhaseName Field',
            name='bldgs_phase_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param10.parameterDependencies = [param06.name]
        param10.value = "phaseName"

        param11 = arcpy.Parameter(
            displayName='Buildings FolioID Field',
            name='bldgs_folioId_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param11.parameterDependencies = [param06.name]
        param11.value = "folioId"

        param12 = arcpy.Parameter(
            displayName='GDB Table ProjectName Field',
            name='gdb_table_project_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param12.parameterDependencies = [param08.name]
        param12.value = "ProjectName"

        param13 = arcpy.Parameter(
            displayName='GDB Table PhaseName Field',
            name='gdb_table_phase_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param13.parameterDependencies = [param08.name]
        param13.value = "PhaseName"

        param14 = arcpy.Parameter(
            displayName='GDB Table FolioNumber Field',
            name='gdb_table_folioNumber_name',
            datatype='Field',
            parameterType='Required',
            direction='Input'
        )
        param14.parameterDependencies = [param08.name]
        param14.value = "FolioNumber"

        GDB_Table_name = arcpy.Describe(param08.valueAsText).basename.split('.')[-1]
        Buildings_name = arcpy.Describe(param06.valueAsText).basename.split('.')[-1]

        # name of the connection file to the default version
        out_n = "Weaver.sde"
        # name of the version_sde_file to be created for editing
        edit_connection_name = "NoiseMit.sde"
        # name of the version to be created for editing
        edit_version_name = "NoiseMit"

        opt = {"account_authentication": "DATABASE_AUTH",
               "username": param02.valueAsText,
               "password": param03.valueAsText,
               "save_user_pass": "SAVE_USERNAME",
               "database": param04.valueAsText,
               "schema": "#",
               "version_type": "TRANSACTIONAL",
               "version": param05.valueAsText,
               "date": ""}

        plat = "SQL_SERVER"
        # attribute fields on the building feature class that need to be selected by the user
        building_attributes = {"Project Name": param09.valueAsText,
                               "Phase Name": param10.valueAsText,
                               "Folio Number": param11.valueAsText}
        # attribute fields on the weaver geodatabase table that need to be selected by the user
        weaver_attributes = {"Project Name": param12.valueAsText,
                             "Phase Name": param13.valueAsText,
                             "Folio Number": param14.valueAsText}
        # These variables are derived from the user parameters, but should be set in this function
        # to allow for importing into the unittests directly.
        composites = [GDB_Table_name, Buildings_name, out_n, edit_connection_name,
                      edit_version_name, plat, building_attributes, weaver_attributes]

        params = [param0, param01, param02, param03, param04, param05, param06,
                  param07, param08, param09, param10, param11, param12, param13,
                  param14, composites]

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

    def process_parameters(self, parameters):
        # These are the parameters defined by the user
        out_f, inst, uid, pwd, database, p_version, bldgs, \
        SQL_Table, GDB_Table, bldg_projectName, bldg_phaseName, \
        bldg_folioId, gdb_table_projectName, gdb_table_phaseName, \
        gdb_table_folioId  = [p.valueAsText for p in parameters[:-1]]

        # These are the parameters derived from the user inputs
        GDB_Table_name, Buildings_name, out_n, edit_connection_name, edit_version_name, \
        opt, plat, building_attributes, weaver_attributes = [p for p in parameters[-1]]

        final_parameters = {
            "out_f": out_f,
            "inst": inst,
            "uid": uid,
            "pwd": pwd,
            "database": database,
            "P_version": p_version,
            "bldgs": bldgs,
            "SQL_Table": SQL_Table,
            "GDB_Table": GDB_Table,
            "bldg_projectName": bldg_projectName,
            "bldg_phaseName": bldg_phaseName,
            "bldg_folioId": bldg_folioId,
            "gdb_table_projectName": gdb_table_projectName,
            "gdb_table_phaseName": gdb_table_phaseName,
            "gdb_table_folioId": gdb_table_folioId,
            "GDB_Table_name": GDB_Table_name,
            "Buildings_name": Buildings_name,
            "out_n": out_n,
            "edit_connection_name": edit_connection_name,
            "edit_version_name": edit_version_name,
            "opt": opt,
            "plat": plat,
            "building_attributes": building_attributes,
            "weaver_attributes": weaver_attributes
        }

        return final_parameters

    def execute(self, parameters, messages):
        """The method calls classes defined in external files."""

        params = self.process_parameters(parameters=parameters)
        out_f = params["out_f"]
        out_n = params["out_n"]
        plat = params["plat"]
        inst = params["inst"]
        opt = params["opt"]
        SQL_Table = params["SQL_Table"]
        GDB_Table = params["GDB_Table"]
        uid = params["uid"]
        p_version = params["p_version"]
        GDB_Table_name = params["GDB_Table_name"]
        Buildings_name = params["Buildings_name"]
        building_attributes = params["building_attributes"]
        weaver_attributes = params["weaver_attributes"]
        bldgs = params["bldgs"]
        edit_version_name = params["edit_version_name"]

        try:
            connection = SdeConnector(out_f, out_n, plat, inst, opt)
            sde_file = connection.create_sde_connection()

            # These values need to be removed when the user parameters are created
            result = Tool.compare_fields(sql_table=SQL_Table, existing_table=GDB_Table)

            compare_result = result["compare_result"]
            match_fields = result["match_fields"]
            add_rows = result["add_rows"]
            exist_rows = result["exist_rows"]

            arcpy.AddMessage({"compare result": compare_result,
                              "add_rows": len(add_rows),
                              "existing_rows": len(exist_rows)})

            # compare result if True means that changes need to be made to the GDB Table and thus the Buildings
            if compare_result:
                # create VersionManager class object to create new version, connect to it,
                # and create an sde connection file, set as current workspace
                # out_folder, platform, instance, target_sde, version_name, new_name, parent_version
                version_manager = VersionManager(opt, out_f, uid, plat, inst, sde_file,
                                                 edit_version_name, p_version)
                version_manager.clean_previous()
                version_sde_file = version_manager.connect_version()

                if os.path.exists(version_sde_file):
                    arcpy.AddMessage(version_sde_file)
                else:
                    arcpy.AddError("version_sde_file not created")

                editor = da.Editor(version_sde_file)
                editor.startEditing()

                gdb_table = arcpy.ListTables("*{}".format(GDB_Table_name))[0]
                if arcpy.Exists(gdb_table):
                    # create GDBTableUpdater class object
                    weaver_updater = GDBTableUpdater(match_fields, gdb_table, add_rows, exist_rows,
                                                     version_sde_file, editor)

                    # set the number of rows per parcel ID as class property
                    pid_dict = weaver_updater.count_pid()
                    # should return True when editing is complete
                    table_updated = weaver_updater.perform_update()

                    env.workspace = version_sde_file
                    # create BuildingUpdater class object
                    noisemit = arcpy.ListDatasets("*Noise*")[0]
                    dataset_path = os.path.join(env.workspace, noisemit)
                    env.workspace = dataset_path
                    buildings = arcpy.ListFeatureClasses("*{}".format(Buildings_name))
                    if len(buildings) == 1:
                        buildings = buildings.pop()
                        if arcpy.Exists(buildings):
                            env.workspace = version_sde_file
                            buildings = os.path.join(noisemit, buildings)
                            building_updater = BuildingsUpdater(buildings, gdb_table, building_attributes,
                                                                weaver_attributes, version_sde_file, editor)
                            # should return True when editing it complete
                            buildings_updated = building_updater.update_buildings()

                            editor.stopEditing(True)
                            del editor

                            # move edits to the default version, and delete the noisemit version
                            version_manager.rec_post()

                        else:
                            editor.stopEditing(False)
                            del editor
                            arcpy.AddError("The buildings do not exist")
                    else:
                        editor.stopEditing(False)
                        del editor
                        arcpy.AddError("Unable to determine the buildings feature class\
                                       using the version connection")
                else:
                    editor.stopEditing(False)
                    del editor
                    arcpy.AddError("Unable to determine the gdb table\
                                    using the version connection")
            else:
                arcpy.AddMessage("The files are identical, no edits needed")

            # Verify that the edits where posted
            # TODO- determine failproof methods for isolating the changed features and viewing the change
            env.workspace = sde_file
            fields = [x for x in building_attributes.itervalues()]
            cursor = da.SearchCursor(bldgs, fields)
            values = cursor.next()
            del cursor
            print values

            os.remove(sde_file)
            return True

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            arcpy.AddError(repr(traceback.format_exception(exc_type, exc_value,exc_traceback)))
