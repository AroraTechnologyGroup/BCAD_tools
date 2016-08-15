import unittest

from tests import test_buildingsUpdater
from tests import test_functions
from tests import test_gdbTableUpdater
from tests import test_sdeConnector
from tests import test_versionManager

functions_suite = test_functions.suite()

suite1 = test_buildingsUpdater.suite()
suite2 = test_gdbTableUpdater.suite()
suite3 = test_sdeConnector.suite()
suite4 = test_versionManager.suite()


all_suites = unittest.TestSuite([suite1, suite2, suite3, suite4])

runner = unittest.TextTestRunner()
runner.run(functions_suite)
runner.run(all_suites)
