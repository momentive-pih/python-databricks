# Databricks notebook source
dbutils.fs.put("/databricks/scripts/update-image_magic.sh","""
#!/bin/bash
sed -i 's/rights="none" pattern="PDF"/rights="read|write" pattern="PDF"/' /etc/ImageMagick-6/policy.xml""", True)

# COMMAND ----------

dbutils.fs.put("/databricks/scripts/update-tesseract.sh","""
#!/bin/bash
sudo add-apt-repository -y ppa:alex-p/tesseract-ocr
sudo apt update
sudo apt-get -q -y install tesseract-ocr""", True)

# COMMAND ----------

dbutils.fs.put("/databricks/scripts/install-pyodbc.sh","""
#!/bin/bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list 
apt-get update
ACCEPT_EULA=Y apt-get install msodbcsql17
apt-get -y install unixodbc-dev
sudo apt-get install python3-pip -y
pip3 install --upgrade pyodbc
/databricks/python/bin/pip install pyodbc""", True)