import unittest
import test_buildingsUpdater
import test_functions
import test_gdbTableUpdater
import test_sdeConnector
import test_versionManager

suite1 = test_buildingsUpdater.suite()
suite2 = test_functions.suite()
suite3 = test_gdbTableUpdater.suite()
suite4 = test_sdeConnector.suite()
suite5 = test_versionManager.suite()


all_suites = unittest.TestSuite([suite1, suite2, suite3, suite4, suite5])

runner = unittest.TextTestRunner()
runner.run(all_suites)
