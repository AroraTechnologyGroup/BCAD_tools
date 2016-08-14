import UpdateNoiseMitSDE as Tool
import unittest
from unittest import TestCase
from BCAD_NoiseMit_Tools import WeaverGDBUpdate as PythonTool
import arcpy
import UpdateNoiseMitSDE as Code
from UpdateNoiseMitSDE import SdeConnector as Connector
import os


class TestClean_row(TestCase):
    def test_clean_row(self):
        test_row = ["   ", None, "  apple", "tree  "]
        row = Tool.clean_row(test_row)
        print row
        self.assertListEqual(["", "unknown", "apple", "tree"], row)


class TestCompare_fields(TestCase):
    def setUp(self):
        """out_folder, out_name, platform, instance, options"""
        tool = PythonTool()
        parameters = tool.getParameterInfo()
        params = tool.process_parameters(parameters=parameters)
        out_folder = params["out_folder"]
        out_name = params["out_n"]
        plat = params["plat"]
        inst = params["inst"]
        opt = params["opt"]
        connector = Connector(out_folder=out_folder, out_name=out_name, platform=plat,
                              instance=inst, options=opt)
        self.sde_file = connector.create_sde_connection()
        self.params = params

    def tearDown(self):
        try:
            os.remove(self.sde_file)
        except:
            pass
        self.params = None

    def test_compare_fields(self):
        params = self.params
        SQL_Table = params["SQL_Table"]
        GDB_Table = params["GDB_Table"]
        for x in [SQL_Table, GDB_Table]:
            if not arcpy.Exists(x):
                self.fail()
        compare = Code.compare_fields(sql_table=SQL_Table, existing_table=GDB_Table)
        keys = compare.keys()
        keys.sort()
        self.assertListEqual(["add_rows", "compare_result",
                              "exist_rows", "match_fields"], keys)

        if compare["compare_result"] == 0:
            for x in [compare["add_rows"], compare["exist_rows"]]:
                self.assertEquals(len(x), 0)

        elif compare["compare_result"] >= 1:
            for x in [compare["add_rows"], compare["exist_rows"]]:
                self.assertGreaterEqual(len(x), 1)


def suite():
    x = unittest.TestLoader().loadTestsFromTestCase(TestClean_row)
    y = unittest.TestLoader().loadTestsFromTestCase(TestCompare_fields)
    all_suites = unittest.TestSuite([x, y])
    return all_suites

if __name__ == '__main__':
    unittest.main()
