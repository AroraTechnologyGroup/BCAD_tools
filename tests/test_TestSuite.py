import unittest

from tests import test_buildingsUpdater
from tests import test_functions
from tests import test_gdbTableUpdater
from tests import test_sdeConnector
from tests import test_versionManager
from tests import test_PythonToolbox


functions_suite = test_functions.suite()

suite1 = test_sdeConnector.suite()
suite2 = test_versionManager.suite()
suite3 = test_gdbTableUpdater.suite()
suite4 = test_buildingsUpdater.suite()
suite5 = test_PythonToolbox.suite()

conn_suites = unittest.TestSuite([suite1, suite2])
data_suites = unittest.TestSuite([suite3, suite4])
master_suites = unittest.TestSuite([suite5])

runner = unittest.TextTestRunner()
# runner.run(functions_suite)
# runner.run(conn_suites)
# runner.run(data_suites)
runner.run(master_suites)
