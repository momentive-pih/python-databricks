# Databricks notebook source
import glob
import configparser
import logging
config = configparser.ConfigParser()
#This configuration path should be configured in Blob storage
config.read("/dbfs/mnt/momentive-configuration/config_test.ini")
name = config.get('mount_path', 'name')
logger = logging.getLogger('momentive_test.log')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("test.log", 'w')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)
try:
  print(name)
  logger.info('hello world')
except Exception as e:
  logger.error('Momentive in critical',exc_info=True)


# COMMAND ----------

# MAGIC %sh
# MAGIC cat /databricks/driver/test.log

# COMMAND ----------

