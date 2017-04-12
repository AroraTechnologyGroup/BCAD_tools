import os
import logging
import datetime
from BCAD_NoiseMit_Tools import CARsGDBUpdate, WeaverGDBUpdate
logfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs/task_log.txt")
logging.basicConfig(filename=logfile, filemode='a', level=30)

cars = CARsGDBUpdate()
params = cars.getParameterInfo()
result = cars.execute(params, "#")
if not result:
    logging.error("{} :: CARsGDBUpdate failed, run geoprocessing tool in ArcMap to debug".format(datetime.datetime.now()))

weav = WeaverGDBUpdate()
params = weav.getParameterInfo()
result = weav.execute(params, "#")
if not result:
    logging.error("{} :: WeaverGDBUpdate failed, run geoprocessing tool in ArcMap to debug".format(datetime.datetime.now()))





