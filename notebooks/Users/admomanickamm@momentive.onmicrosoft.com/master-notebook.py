# Databricks notebook source
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

import multiprocessing as mp
from multiprocessing.pool import ThreadPool
import datetime
import os
import re
import pandas as pd
import numpy as np
import datetime
import re
import configparser
import logging
import os
import nltk

#nltk initializer 
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger')
nltk.download('punkt')
from nltk.tokenize import sent_tokenize
from nltk.corpus import stopwords
#stop word removal
stop_words=list(set(stopwords.words("english")))

#Loging environment setup
logger = logging.getLogger('momentive')
logger.setLevel(logging.DEBUG)
#This log path should be configured in blob storage
fh = logging.FileHandler("sales_force.log", 'w')
fh.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter(fmt = '%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
# add formatter to fh
fh.setFormatter(formatter)
# add fh to logger
logger.addHandler(fh)

config = configparser.ConfigParser()
#This configuration path should be configured in Blob storage
config.read("/dbfs/mnt/momentive-configuration/config-file.ini")
sfdc_splitted_folder = config.get('mnt_sales_force','mnt_sales_force_split_files')
filename = config.get('mnt_sales_force','mnt_sales_force_out_filename')
sfdc_case_email_table_name = config.get('mnt_sales_force',"mnt_sales_force_case_email_table_name")
sfdc_identified_case = config.get('mnt_sales_force',"mnt_sfdc_identifed_table_name")
sfdc_extract_column = config.get('mnt_sales_force',"mnt_sales_force_extract_column")
sfdc_column = sfdc_extract_column.split(",")
sfdc_info_validate_column = config.get('mnt_sales_force',"mnt_sales_force_validate_column")
sfdc_validate_column  = sfdc_info_validate_column.split(",")
inscope_product_validate_column=config.get('mnt_sales_force',"mnt_product_inscope_column")
validate_product_column = inscope_product_validate_column.split(",")
inscope_product=(config.get('mnt_sales_force',"mnt_product_string")).split(",")
sfdc_identified_info_query=config.get('mnt_sales_force',"mnt_sfdc_identified_case_info_query")
product_info_query = config.get('mnt_sales_force',"mnt_product_information_query")
historical_query = config.get('mnt_sales_force',"mnt_sales_force_historical_query")
incremental_query = config.get('mnt_sales_force',"mnt_sales_force_incremental_query")
table_connector="momentive"
view_connector="dbo"
modified_column = config.get("mnt_sales_force","mnt_sales_force_last_modified_columm")
adding_custom_column=['MatchedColumn','MatchedCategory','MatchedValue']
sfdc_new_validate_column=["validate_category"]

def main():
  try:
    mat_prod=''
    mat_bdt=''
    mat_desc=''
    count=0
    final_list=pd.DataFrame()
    flag_final=pd.DataFrame()
    re_match=pd.DataFrame()
    exact_match=pd.DataFrame()
    wh_pat_column=pd.DataFrame()
    df_each_column=pd.DataFrame()
    c_count=0
    f_count=0
    old_file_flag=''
    query1=''  
    prod_flag=''
    
    #Connecting SQL db to get SFDC data
    sql_cursor = SQL_connection("server","database","username","password")
    cursor=sql_cursor.cursor()
    
    #Incope product
#     product_info_df = pd.read_sql(product_info_query, sql_cursor)
    product_info_df = pd.read_csv('/dbfs/mnt/momentive-sources-pih/sales-force/product_info_V2.csv',encoding="ISO-8859-1")
    product_info_df=product_info_df.fillna("NULL")
    
    product_columns=product_info_df.columns
    for item in product_columns:
      product_info_df[item]=product_info_df[item].astype('str').str.strip()
    
    product_info_df = product_info_df[217:218]
    #Identifed case table
    identified_sfdc_df = pd.read_sql(sfdc_identified_info_query, sql_cursor)
    
    #Historical and incremental logic
    if len(identified_sfdc_df)>0:
        try:
          identified_sfdc_df[modified_column]=pd.to_datetime(identified_sfdc_df[modified_column])
          last_modified_date = str(identified_sfdc_df[modified_column].max())[:-3]
          if last_modified_date !='':
            print("last_modified",last_modified_date)
            old_file_flag='s'
            #query execution
            detect_sfdc_info_query = incremental_query+"'"+ str(last_modified_date) +"')"
          else:
            detect_sfdc_info_query=historical_query
        except Exception as e:
          logger.error("Error in sales force : Injecting SFDC ticket",exc_info=True)
          
    if old_file_flag =='s':
      old_file_flag=''
    else:
      detect_sfdc_info_query = historical_query
      
    #loading SFDC data into dataframe
#     inscope_sfdc_info_df = pd.read_sql(detect_sfdc_info_query, sql_cursor)
#     inscope_sfdc_info_df=inscope_sfdc_info_df.fillna("NULL")
#     inscope_sfdc_info_df=inscope_sfdc_info_df.replace({"None":"NULL"})
    
    #remove multiple whitespace with single space for validate column
    def optimize_function(column_value):
      big_regex = re.compile('|'.join(map(re.escape, to_remove_from_list)))
      string_join=big_regex.sub("", column_value)
      filtered_item=string_join.split()
      filtered_stop_words = [item for item in filtered_item if item not in stop_words]
      tag_item=nltk.pos_tag(filtered_stop_words)
      final_str=''
      pos_filter=[word for word,tag in tag_item if tag in ["NNP","NN","CD"]]
      final_str=" ".join(pos_filter)
      return final_str
    
#     to_remove_from_list=["momentive","com",'?',"@","*","€","â","”","!","https","www"]    
#     inscope_sfdc_info_df["validate_category"] = inscope_sfdc_info_df[sfdc_validate_column].apply(lambda x: ' '.join(x), axis = 1) 
#     inscope_sfdc_info_df["validate_category"] =inscope_sfdc_info_df["validate_category"].apply(optimize_function) 
    inscope_sfdc_info_df=pd.read_csv('/dbfs/mnt/momentive-sources-pih/sales-force/backup/test.csv',encoding="ISO-8859-1")
  
    check_product_column=["Text1","Text2","Text3"]   
    starting_indx=-1
    
    if len(product_info_df)>0 and len(inscope_sfdc_info_df)>0:
      for column_type in check_product_column:
        try:
          category=["Type",column_type,"SUBCT"]
          df_checked=product_info_df[category]
          df_checked.drop_duplicates(inplace=True)
          to_be_checked=df_checked.values.tolist()
          starting_indx+=1
          argument_str=[]
          for category_type,item,subct in to_be_checked:
            try:
              temp_str=category_type+","+str(item)+","+subct+","+str(starting_indx)
              argument_str.append(temp_str)
            except Exception as e:
              logger.error("Error in sales force",exc_info=True)   
          print(argument_str)
          pool = ThreadPool(25)
          pool.map(lambda path:dbutils.notebook.run('/Users/admomanickamm@momentive.onmicrosoft.com/parallel_process',timeout_seconds=0,arguments = {"to_checked":path}),argument_str) 
          pool.close()
        except Exception as e:
          logger.error("Error in sales force",exc_info=True)
    
  except Exception as e:
    logger.error("Error in sales force",exc_info=True)


if __name__ == '__main__':
  main()



# COMMAND ----------

# MAGIC %sh
# MAGIC cat /databricks/driver/sales_force.log

# COMMAND ----------

product_info_df = pd.read_csv('/dbfs/mnt/momentive-sources-pih/sales-force/product_info_V2.csv',encoding="ISO-8859-1")
    product_info_df = product_info_df[300:301]
    inscope_sfdc_info_df=pd.read_csv('/dbfs/mnt/momentive-sources-pih/sales-force/backup/test.csv',encoding="ISO-8859-1")
    inscope_sfdc_info_df=inscope_sfdc_info_df.fillna("NULL")
    inscope_sfdc_info_df=inscope_sfdc_info_df.replace({"None":"NULL"})
    output_df=pd.DataFrame()
    check_product_column=["Text1","Text2","Text3"]
    if len(product_info_df)>0 and len(inscope_sfdc_info_df)>0:
      for col in check_product_column:
        sfdc_temp_df=inscope_sfdc_info_df.copy()
        colu=["Type",col,"SUBCT"]
        df_checked=product_info_df[colu]
        df_checked.drop_duplicates(inplace=True)
        to_checked=df_checked.values.tolist()
        added_str=[]
        path="sucess"
        for ty,val,subct in to_checked:
          test_str=ty+","+val+","+subct
          added_str.append(test_str)
        print(added_str)
        pool = ThreadPool(2) 
        
#         pool.map(lambda type1,value,subct:dbutils.notebook.run('/Users/admomanickamm@momentive.onmicrosoft.com/parallel_process',timeout_seconds=0,arguments = {"type1":type1,"value":value,"subct":subct}),to_checked)
        pool.map(lambda path:dbutils.notebook.run('/Users/admomanickamm@momentive.onmicrosoft.com/parallel_process',timeout_seconds=0,arguments = {"to_checked":path,"path":path}),added_str)
#         print("next")
#         pool.close()
      