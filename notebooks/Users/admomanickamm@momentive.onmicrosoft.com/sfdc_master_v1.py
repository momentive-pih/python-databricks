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
import re
import configparser
import logging
import os
import json
import nltk

#nltk initializer 
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger')
nltk.download('punkt')
from nltk.tokenize import sent_tokenize
from nltk.corpus import stopwords
#stop word removal
stop_words=list(set(stopwords.words("english")))
current_date = str(datetime.datetime.now())
date = current_date[:10]
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
#output file names
sfdc_text_folder = config.get('mnt_sales_force','mnt_sales_force_split_files')
history_filename = config.get('mnt_sales_force','mnt_sales_force_historical_filename')
incremental_filename = config.get('mnt_sales_force','mnt_sales_force_incremental_filename')
product_filename=config.get('mnt_sales_force','mnt_product_information_filename')
#sql queries info
sfdc_identified_info_query=config.get('mnt_sales_force',"mnt_sfdc_identified_case_info_query")
product_info_query = config.get('mnt_sales_force',"mnt_product_information_query")
ontology_info_query = config.get('mnt_sales_force',"mnt_ontology_product_query")
incremental_ontology_query = config.get('mnt_sales_force',"mnt_ontology_incremental_product_query")
historical_query = config.get('mnt_sales_force',"mnt_sales_force_historical_query")
ontology_update_query = config.get('mnt_sales_force',"mnt_ontology_update_query")
table_connector="momentive"
view_connector="dbo"
#column names
sfdc_extract_column = config.get('mnt_sales_force',"mnt_sales_force_extract_column")
sfdc_column = sfdc_extract_column.split(",")
sfdc_info_validate_column = config.get('mnt_sales_force',"mnt_sales_force_validate_column")
sfdc_validate_column  = sfdc_info_validate_column.split(",")
inscope_product_validate_column=config.get('mnt_sales_force',"mnt_product_inscope_column")
validate_product_column = inscope_product_validate_column.split(",")
case_modified_column = config.get("mnt_sales_force","mnt_sales_case_last_modified_column")
email_modified_column = config.get("mnt_sales_force","mnt_sales_email_last_modified_column")
adding_custom_column=config.get("mnt_sales_force","mnt_adding_custom_column_sdfc")
adding_custom_column=adding_custom_column.split(",")
sfdc_new_validate_column=config.get("mnt_sales_force","mnt_adding_merge_column")
sfdc_new_validate_column=sfdc_new_validate_column.split(",")
product_category=config.get('mnt_sales_force',"mnt_product_category")
selected_product_type=product_category.split(",")


def path_exists(file_path):
  try:
    logger.info("Executing path_exists function")
    dbutils.fs.rm(file_path.replace("/dbfs",""),True)
    dbutils.fs.mkdirs(file_path.replace("/dbfs","dbfs:"))
  except Exception as e:
    logger.error("Error in path_exists function : ",exc_info=True)

def main():
  try:
    mat_prod=''
    mat_bdt=''
    mat_desc=''
    count=0
    product_info_df=pd.DataFrame()
    ontology_product_df=pd.DataFrame()
    inc_ontology_product_df=pd.DataFrame()
    identified_sfdc_df=pd.DataFrame()  
    inscope_sfdc_info_df=pd.DataFrame()
    c_count=0
    f_count=0
    old_file_flag=''
    query1=''  
    prod_flag=''
    processing_file_name=''  
    history_flag='true'
    try:
      #Connecting SQL db to get SFDC data
      sql_cursor = SQL_connection("server","database","username","password")
      cursor=sql_cursor.cursor()
      
      #Incope product
      product_info_df = pd.read_sql(product_info_query, sql_cursor)
      print("sql_product_count --> ",len(product_info_df)) 
      #writing product into file
      if not os.path.exists(sfdc_text_folder):
        path_exists(sfdc_text_folder)
      product_info_df["Type"]=product_info_df["Type"].str.strip()
      product_info_df["Text1"]=product_info_df["Text1"].str.strip()
      product_path=sfdc_text_folder+product_filename+".csv"
      product_info_df.to_csv(product_path,index=False)
      ##########################
      product_info_df = product_info_df[product_info_df["Type"].isin(selected_product_type)]
      product_info_df.drop_duplicates(inplace=True)
      print("filtered product count --> ",len(product_info_df))   
      product_info_df=product_info_df[0:200]
      product_info_df=product_info_df.fillna("NULL")
      
      #ontology product
      ontology_product_df=pd.read_sql(ontology_info_query, sql_cursor)
      print("sql_ontology_product_count --> ",len(ontology_product_df))
      ontology_product_df.drop_duplicates(inplace=True)
      ontology_product_df=ontology_product_df.fillna("NULL")
      inc_ontology_product_df=pd.read_sql(incremental_ontology_query, sql_cursor)
      print("sql_incremental_ontology_product_count --> ",len(inc_ontology_product_df))
      inc_ontology_product_df.drop_duplicates(inplace=True)
      inc_ontology_product_df=inc_ontology_product_df.fillna("NULL")
      
      #Identifed case table
      identified_sfdc_df = pd.read_sql(sfdc_identified_info_query, sql_cursor)
      print("sql_identified sfdc record count --> ",len(identified_sfdc_df))
      identified_sfdc_df=identified_sfdc_df.fillna("NULL")
      
    except Exception as e:
      logger.error("Error in accessing db : ",exc_info=True)
    
    #converting all column type into string
    product_columns=product_info_df.columns
    for item in product_columns:
      product_info_df[item]=product_info_df[item].astype('str').str.strip()
    
    #Historical and incremental logic
    if len(identified_sfdc_df)>0:
        try:
          case_last_modified_date=''
          email_last_modified_date=''
          where_condition=''
          identified_sfdc_df[case_modified_column]=pd.to_datetime(identified_sfdc_df[case_modified_column],errors='coerce')
          identified_sfdc_df[email_modified_column]=pd.to_datetime(identified_sfdc_df[email_modified_column],errors='coerce')
          case_last_modified_date = str(identified_sfdc_df[case_modified_column].max())[:-3]        
          email_last_modified_date = str(identified_sfdc_df[email_modified_column].max())[:-3]
          if case_last_modified_date !='' or email_last_modified_date!='':
            print("case_last_modified_date",case_last_modified_date)
            print("email_last_modified_date",email_last_modified_date)
            history_flag='false'          
            casedate=case_modified_column+" > convert(datetime,'"+case_last_modified_date+"')"
            where_condition=" where ("+casedate
            if email_last_modified_date!='':
              emaildate=email_modified_column+" > convert(datetime,'"+email_last_modified_date+"')"
              where_condition=where_condition+" or "+emaildate
            where_condition=where_condition+")"
            #query execution
            detect_sfdc_info_query = historical_query+where_condition
          else:
            detect_sfdc_info_query=historical_query
        except Exception as e:
          logger.error("Error in sales force : Injecting SFDC ticket",exc_info=True)
          
    if history_flag =='false':
      filename=incremental_filename
    else:
      filename=history_filename
      detect_sfdc_info_query = historical_query
    print(detect_sfdc_info_query)
#     detect_sfdc_info_query = historical_query
    
#     try:
#       #loading SFDC data into dataframe
#       inscope_sfdc_info_df = pd.read_sql(detect_sfdc_info_query, sql_cursor)
#       print("sql_sfdc_count --> ",len(inscope_sfdc_info_df)) 
#       inscope_sfdc_info_df=inscope_sfdc_info_df.fillna("NULL")
#       inscope_sfdc_info_df=inscope_sfdc_info_df.replace({"None":"NULL"})
#     except Exception as e:
#       logger.error("Error in accessing sfdc inscope db : ",exc_info=True)
    
    #remove multiple whitespace with single space for validate column
#     def optimize_function(column_value):
#       big_regex = re.compile('|'.join(map(re.escape, to_remove_from_list)))
#       string_join=big_regex.sub("", column_value)
#       filtered_item=string_join.split()
#       filtered_stop_words = [item for item in filtered_item if item not in stop_words]
# #       tag_item=nltk.pos_tag(filtered_stop_words)
#       final_str=''        
# #       pos_filter=[word for word,tag in tag_item if tag in ["NNP","NN","CD"]]
#       final_str=" ".join(filtered_stop_words)
#       return final_str
    
#     to_remove_from_list=["momentive","com",'?',"@","*","€","â","”","!","https","www"] 
#     for item in sfdc_validate_column:
# #     inscope_sfdc_info_df["validate_category"] = inscope_sfdc_info_df[sfdc_validate_column].apply(lambda x: ' '.join(x), axis = 1) 
#       inscope_sfdc_info_df[item] =inscope_sfdc_info_df[item].apply(optimize_function)
#     print("filtered_sfdc_count -->",len(inscope_sfdc_info_df))
    
    #writing sfdc data into blob storage for passing file to concurrent file process    
    if not os.path.exists(sfdc_text_folder):
        path_exists(sfdc_text_folder)
    processing_file_name = sfdc_text_folder+filename+".csv"
#     print("file - ",processing_file_name)
#     inscope_sfdc_info_df.to_csv(processing_file_name,index=False)
    inscope_sfdc_info_df=pd.read_csv(processing_file_name)
  
    check_product_column=["Text1","Text2","Text3"] 
    check_ontology_column=["ontology_key","ontology_value","key_type","processed_flag"]
    row_product=[]
    argument_str=[]
    starting_indx=-1
    
    def multiprocess_function(pass_value):
      try:
        status=dbutils.notebook.run('/Users/admomanickamm@momentive.onmicrosoft.com/sfdc_parallel',timeout_seconds=0,arguments = {"to_be_checked":pass_value})
        print(status)
        logger.info(status)
      except Exception as e:
        logger.error("Error in parallel processing",status)
    
    def creating_argument_value(input_value,starting_indx,file_indication):
      try:
        temp_row=[]
        for category_type,item,subct,specid in input_value:
          try:
            item=str(item.strip())
            item=item.replace("'","")
            category_type=str(category_type.strip())
            subct=str(subct.strip())          
            if (item !='' and item.lower() !="null"):
              if file_indication !="both":
                temp_str=category_type+"---"+item+"---"+subct+"---"+str(starting_indx)+"---"+str(specid.strip())+"---"+file_indication
                temp_row.append(temp_str)
          except Exception as e:          
            logger.error("Error in creating_argument_value",exc_info=True)
        return temp_row
      except Exception as e:
        logger.error("Error in creating_argument_value",exc_info=True)
        
    def update_ontology(to_be_checked):
      for ontology_key,ontology_value,key_type,pflag in to_be_checked:
        try:
          update_query=ontology_update_query+"'"+ontology_value+"'"
          cursor.execute(update_query)
          sql_cursor.commit()
        except Exception as e:
          logger.error("Error in updating_ontology_value",exc_info=True)
          
    if (len(inscope_sfdc_info_df)>0 and os.path.exists(processing_file_name)):
      if history_flag=="true":
          file_indication="history"
      else:
        file_indication="incremental"
      if len(product_info_df)>0:    
        for column_type in check_product_column:
          try:
            category=["Type",column_type,"SUBCT","Text2"]
            df_checked=product_info_df[category]
            df_checked.drop_duplicates(inplace=True)
            to_be_checked=df_checked.values.tolist()
            starting_indx+=1   
            argument_str=creating_argument_value(to_be_checked,starting_indx,file_indication)
            row_product=row_product+argument_str
          except Exception as e:
            logger.error("Error in sales force",exc_info=True)
      if len(ontology_product_df)==len(inc_ontology_product_df):
        print(len(ontology_product_df))
        ontology_df=ontology_product_df[check_ontology_column]
        ontology_df.drop_duplicates(inplace=True)
        to_be_checked=ontology_df.values.tolist()
        starting_indx="ontology"
        argument_str=creating_argument_value(to_be_checked,starting_indx,file_indication)
        row_product=row_product+argument_str
        update_ontology(to_be_checked)
      elif len(inc_ontology_product_df)>0:
        inc_ontology_df=inc_ontology_product_df[check_ontology_column]
        inc_ontology_df.drop_duplicates(inplace=True)
        to_be_checked=inc_ontology_df.values.tolist()
        starting_indx="ontology"
        argument_str=creating_argument_value(to_be_checked,starting_indx,file_indication)
        row_product=row_product+argument_str
        update_ontology(to_be_checked)
        
    elif len(inc_ontology_product_df) != len(inc_ontology_product_df):
      if os.path.exists(sfdc_text_folder+history_filename+".csv"):
        file_indication="history"
        inc_ontology_df=inc_ontology_product_df[check_ontology_column]
        inc_ontology_df.drop_duplicates(inplace=True)
        to_be_checked=inc_ontology_df.values.tolist()
        starting_indx="ontology"
        argument_str=creating_argument_value(to_be_checked,starting_indx,file_indication)
        row_product=row_product+argument_str
        update_ontology(to_be_checked)
#       print(row_product)    
      #calling notebook for concurrent process 
#     print("row",row_product)
    if len(row_product)>0:
      thread_length=int((len(row_product))/2)
      if thread_length>48:
        thread_length=50
      print("thread_length",thread_length)
      pool = ThreadPool(thread_length)
#       pool = ThreadPool(10)
      logger.info("started parallel processing")
      pool.map(multiprocess_function,row_product)
      pool.close()
      
    #overwrite_old_history_file
    if histroy_flag=="false" and os.path.exists(processing_file_name) and len(inscope_sfdc_info_df)>0:
      history_df=pd.read_csv(sfdc_text_folder+history_filename+".csv",encoding="ISO-8859-1")
      history_df = pd.concat([history_df,inscope_sfdc_info_df])
      history_df.to_csv(sfdc_text_folder+history_filename+".csv",index=False)
    
  except Exception as e:
    logger.error("Error in sales force",exc_info=True)


if __name__ == '__main__':
  main()


# COMMAND ----------

# MAGIC %sh
# MAGIC cat /databricks/driver/sales_force.log

# COMMAND ----------

