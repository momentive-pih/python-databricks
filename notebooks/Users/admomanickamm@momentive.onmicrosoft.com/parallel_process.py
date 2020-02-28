# Databricks notebook source
c_value=dbutils.widgets.get("to_checked")

# COMMAND ----------

# Databricks notebook source
def SQL_connection(server,database,username,password):
  import pyodbc
  import configparser
  import traceback

  config = configparser.ConfigParser()
  #This configuration path should be configured in Blob storage
  config.read("/dbfs/mnt/momentive-configuration/config-file.ini")

  server = config.get('sql_db', server)
  database = config.get('sql_db', database)
  username = config.get('sql_db', username)
  password = config.get('sql_db', password)
  

  driver= "{ODBC Driver 17 for SQL Server}"
  connection_string = 'DRIVER=' + driver + \
                      ';SERVER=' + server + \
                      ';PORT=1433' + \
                      ';DATABASE=' + database + \
                      ';UID=' + username + \
                      ';PWD=' + password

  try:
      sql_conn = pyodbc.connect(connection_string)
      return sql_conn
      # execute query and save data in pandas df
  except Exception as error:
      print("    \u2717 error message: {}".format(error))
      # I found that traceback prints much more detailed error message
      traceback.print_exc()

# COMMAND ----------

def adding_matched_values(temp_df,category_type,indx,value,subct):
  try:
    print("type",category_type,"indx",indx,"value",value,"subct",subct) 
    indx=int(indx)
    matched_category=''
    matched_column=''
    if category_type == "MATNBR":
      if indx==0:
        matched_category="MATERIAL NUMBER"
        matched_column="Text1"   
      elif indx==1:
        matched_category="REAL-SPECID"
        matched_column="Text2"   
      else:
        matched_category="BDT"
        matched_column="Text3"

    elif category_type == "NAMPROD":
      if indx==0:
        matched_category="NAMPROD"
        matched_column="Text1" 
      elif indx==1 and subct=="REAL_SUB":
        matched_category="REAL-SPECID"
        matched_column="Text2"   
      elif indx==1 and subct=="PURE_SUB":
        matched_category="PURE-SPECID"
        matched_column="Text2"   
      else:
        matched_category="SYNONYMS"
        matched_column="Text3"
    elif category_type == "NUMCAS":
      if indx==0:
        matched_category="NUMCAS"
        matched_column="Text1"  
      elif indx==1 and subct=="REAL_SUB":
        matched_category="REAL-SPECID"
        matched_column="Text2"   
      elif indx==1 and subct=="PURE_SUB":
        matched_category="PURE-SPECID"
        matched_column="Text2" 
      else:
        matched_category="CHEMICAL NAME"
        matched_column="Text3"
#         print("matched_category",matched_category)
#         print("matched_column",matched_column)
    temp_df["MatchedColumn"]=matched_column
    temp_df["MatchedCategory"]=matched_category
    temp_df["MatchedValue"]=value
    print(matched_column,matched_category,value)
    return temp_df
  except Exception as e:
    print("error in adding matched_values",e)

# COMMAND ----------

import multiprocessing as mp
from multiprocessing.pool import ThreadPool
import datetime
import os
import re
import pandas as pd
import configparser
import datetime

current_date = str(datetime.datetime.now())
date = current_date[:10]
config = configparser.ConfigParser()
#This configuration path should be configured in Blob storage
config.read("/dbfs/mnt/momentive-configuration/config-file.ini")
filename = config.get('mnt_sales_force','mnt_sales_force_out_filename')
sfdc_text_folder = config.get('mnt_sales_force','mnt_sales_force_split_files')
sfdc_extract_column = config.get('mnt_sales_force',"mnt_sales_force_extract_column")
sfdc_column = sfdc_extract_column.split(",")
# inscope_sfdc_info_df=pd.read_csv('/dbfs/mnt/momentive-sources-pih/sales-force/backup/test.csv',encoding="ISO-8859-1")
inscope_sfdc_info_df=pd.read_csv(sfdc_text_folder+filename+".csv",encoding="ISO-8859-1")
print("processing file length - ",len(inscope_sfdc_info_df))
#Connecting SQL db to get SFDC data
sql_cursor = SQL_connection("server","database","username","password")
cursor=sql_cursor.cursor()
adding_custom_column=['MatchedColumn','MatchedCategory','MatchedValue']
cvalue=c_value.split(",")
print("cvalue is -- ",cvalue)
output_df=pd.DataFrame()
custom_validate=["validate_category"]

def concurrent_function(cvalue):
  try:
    global output_df
    validate="validate_category"    
    item=cvalue[1]
    category_type=cvalue[0]
    subct=cvalue[2]
    indx=cvalue[3]
    print("indx",cvalue[3])
    org_value=str(item)   
    value=org_value.strip().lower()
    if value.isdigit() and len(value)>0 :  
      value=int(value)
      print(value)
      rgx = re.compile(r'((?<!lsr)(?<!silsoft)(?<!\d)(^|\s+|#){}(\D|$))'.format(value),re.I)  
      re_match=inscope_sfdc_info_df[inscope_sfdc_info_df[validate].str.contains(rgx,na=False)]               
      if len(re_match)>1: 
          print("matched digit",item)
          digit_match_row=adding_matched_values(re_match,category_type,indx,org_value,subct)
          output_df=pd.concat([output_df,digit_match_row])
    elif len(value)>0 and ("?" not in value and "!" not in value):
      value=value.replace("silopren*",'')
      e_value=value.replace("*",'')
      e_value=e_value.replace("®",'')
      whole_match=pd.DataFrame()
      w_rgx = re.compile(r"(([^a-zA-Z]|^){}([^a-zA-Z]|$))".format(e_value),re.I)
      whole_match=inscope_sfdc_info_df[inscope_sfdc_info_df[validate].str.contains(w_rgx,na=False)]    
      if len(whole_match)>0:
        print("matched",item)
        string_match_column=adding_matched_values(whole_match,category_type,indx,org_value,subct)
        output_df=pd.concat([output_df,string_match_column])
  except Exception as e:
    print("value error",e)

concurrent_function(cvalue)
# inserting into sfdc indentified table
if len(output_df)>0:
  output_df.drop_duplicates(inplace=True)
  output_df=output_df[(sfdc_column+adding_custom_column)]
  output_df=output_df.fillna("NULL")
  output_df=output_df.replace({"None":"NULL"})
  cursor=sql_cursor.cursor()
  output_list = output_df.values.tolist()
  print(len(output_list))
  for row in output_list:
    try:            
      insert_data=''
      for item in row:
        item=str(item)
        if "'" in item:
          item=item.replace("'","''")
        insert_data+="'"+item+"',"
      if len(insert_data)>0:
        insert_data=insert_data[:-1]
        insert_query="insert into [momentive].[test_sfdc_identified_case] values ("+insert_data+")"
        cursor.execute(insert_query)
        sql_cursor.commit()
    except Exception as e:
      print("value error",e)
