# Databricks notebook source
#Python code for unmounting
dbutils.fs.unmount('/mnt/momentive-configuration')
dbutils.fs.unmount('/mnt/momentive-pih-logging')
dbutils.fs.unmount('/mnt/momentive-source')

# COMMAND ----------

