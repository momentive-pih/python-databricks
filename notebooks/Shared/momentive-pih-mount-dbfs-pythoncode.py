# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://momentive-configuration@clditdevstorpih.blob.core.windows.net",
  mount_point = "/mnt/momentive-configuration",
  extra_configs = {"fs.azure.account.key.clditdevstorpih.blob.core.windows.net":"LY6FMa0XjZ38ODlIMCxsXVBCpvJpqxwFViNrfquehTmUqDZIosM1BO7DwMLZqCTAw2plt5qN7jAqF1Hw28U/pw=="})

dbutils.fs.mount(
  source = "wasbs://momentive-pih-logging@clditdevstorpih.blob.core.windows.net",
  mount_point = "/mnt/momentive-pih-logging",
  extra_configs = {"fs.azure.account.key.clditdevstorpih.blob.core.windows.net":"LY6FMa0XjZ38ODlIMCxsXVBCpvJpqxwFViNrfquehTmUqDZIosM1BO7DwMLZqCTAw2plt5qN7jAqF1Hw28U/pw=="})

dbutils.fs.mount(
  source = "wasbs://momentive-source@clditdevstoragepih.blob.core.windows.net",
  mount_point = "/mnt/momentive-source",
  extra_configs = {"fs.azure.account.key.clditdevstoragepih.blob.core.windows.net":"LY6FMa0XjZ38ODlIMCxsXVBCpvJpqxwFViNrfquehTmUqDZIosM1BO7DwMLZqCTAw2plt5qN7jAqF1Hw28U/pw=="})
