# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://momentive-configuration@devstorpih001.blob.core.windows.net",
  mount_point = "dbfs/mnt/momentive-configuration",
  extra_configs = {"fs.azure.account.key.devstorpih001.blob.core.windows.net":"hBTtfr+U72t9bsqx9dOBVZ7EiccMV5mcMXQDDPXH45yfTHIg0dOBuHDtwGjuxUP6Wc1zyBVaLws2Zda3AhuaYA=="})

dbutils.fs.mount(
  source = "wasbs://momentive-pih-logging@devstorpih001.blob.core.windows.net",
  mount_point = "dbfs/mnt/momentive-pih-logging",
  extra_configs = {"fs.azure.account.key.devstorpih001.blob.core.windows.net":"hBTtfr+U72t9bsqx9dOBVZ7EiccMV5mcMXQDDPXH45yfTHIg0dOBuHDtwGjuxUP6Wc1zyBVaLws2Zda3AhuaYA=="})

dbutils.fs.mount(
  source = "wasbs://momentive-source@devstorpih001.blob.core.windows.net",
  mount_point = "dbfs/mnt/momentive-source",
  extra_configs = {"fs.azure.account.key.devstorpih001.blob.core.windows.net":"hBTtfr+U72t9bsqx9dOBVZ7EiccMV5mcMXQDDPXH45yfTHIg0dOBuHDtwGjuxUP6Wc1zyBVaLws2Zda3AhuaYA=="})
