from LoggerWrapper import LoggerWrapper
from PysparkWrapper import PysparkWrapper

# Initialize logger
logger = LoggerWrapper.init_logger(__name__)

def insert_to_table(**kwargs):
  logger.debug("Write data into Hive with table name: " + kwargs["input"])
  kwargs["input"]["data"].write.format("hive").mode("append").saveAsTable(kwargs["input"]["target"])
  logger.debug("Successfully write data into " + kwargs["input"]["target"] + " table")
  #kwargs["output"] = kwargs["sqlcontext"].sql("select * from " + kwargs["input"]["target"])
  return kwargs
# end def
