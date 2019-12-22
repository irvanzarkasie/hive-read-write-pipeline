from LoggerWrapper import LoggerWrapper
from PysparkWrapper import PysparkWrapper

# Initialize logger
logger = LoggerWrapper.init_logger(__name__)

def read_from_table(**kwargs):
  logger.debug("Read data from Hive with table name: " + kwargs["input"]["target"])
  sqlcontext = PysparkWrapper.init_processor(kwargs["id"])
  kwargs["output"] = sqlcontext.sql("select * from " + kwargs["input"]["target"])
  return kwargs
# end def
