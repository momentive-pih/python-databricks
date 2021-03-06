# Databricks notebook source
# Databricks notebook source
c_value=dbutils.widgets.get("to_be_checked")

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

def adding_matched_values(temp_df,category_type,indx,value,subct,specid):
  try:
    matched_category=''
    matched_column=''
    global product_info_df
    if indx!="ontology":
      indx=int(indx)   
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
    if subct=="PURE_SUB":
      real_spec_df=product_info_df[(product_info_df["Type"]=="SUBIDREL") & (product_info_df["Text1"]==specid)]
#       print(real_spec_df)
      if len(real_spec_df)>0:
        real_spec_df["Text2"]=real_spec_df["Text2"].str.strip()
        speclist=list(real_spec_df["Text2"].unique())
#         print(speclist)
        specid=";".join(speclist) 
    
    temp_df["MatchedProductValue"]=value
    if indx=="ontology":    
      temp_df["MatchedProductValue"]=category_type
      matched_column="Ontology - "+str(value)
      matched_category=subct
      if subct=="NAMPROD":
#         print("subdct",category_type)
        category_type=str(category_type).strip()
#         real_spec_df_1=product_info_df[(product_info_df["Type"]=="NAMPROD") & (product_info_df["Text1"]==category_type)]
#         print("listvalues",real_spec_df_1.values.tolist())
        real_spec_df=product_info_df[(product_info_df["Type"]=="NAMPROD") & (product_info_df["Text1"]==category_type) & (product_info_df["SUBCT"]=="REAL_SUB  ")]
      elif subct=="BDT":
        real_spec_df=product_info_df[(product_info_df["Type"]=="MATNBR") & (product_info_df["Text3"]==category_type)]
      if len(real_spec_df)>0:
        real_spec_df["Text2"]=real_spec_df["Text2"].str.strip()
        speclist=list(real_spec_df["Text2"].unique())
#         print(speclist)
        specid=";".join(speclist) 
      else:
        specid="ontology"                         
#       specid="ontology"
      
    temp_df["MatchedProductColumn"]=matched_column
    temp_df["MatchedProductCategory"]=matched_category 
    temp_df["RealSpecId"]=specid
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
sfdc_text_folder = config.get('mnt_sales_force','mnt_sales_force_split_files')
history_filename = config.get('mnt_sales_force','mnt_sales_force_historical_filename')
incremental_filename = config.get('mnt_sales_force','mnt_sales_force_incremental_filename')
product_filename=config.get('mnt_sales_force','mnt_product_information_filename')
if os.path.exists(sfdc_text_folder+product_filename+".csv"):
  product_info_df=pd.read_csv(sfdc_text_folder+product_filename+".csv",encoding="ISO-8859-1") 
sfdc_info_validate_column = config.get('mnt_sales_force',"mnt_sales_force_validate_column")
sfdc_validate_column  = sfdc_info_validate_column.split(",")
sfdc_custom_column=config.get('mnt_sales_force',"mnt_adding_custom_column_sdfc")
adding_custom_column=sfdc_custom_column.split(",")
skip_list=config.get('mnt_sales_force',"mnt_skip_list")
skip_list=skip_list.split(",")
sfdc_insert_query=config.get('mnt_sales_force',"mnt_sfdc_insert_query")
cvalue=c_value.split("---")
output_str = "|".join(cvalue)
if len(cvalue)>5:
  if cvalue[-1] =='history':
    filename=history_filename     
  else:
    filename=incremental_filename    
sfdc_extract_column = config.get('mnt_sales_force',"mnt_sales_force_extract_column")
sfdc_column = sfdc_extract_column.split(",")
if os.path.exists(sfdc_text_folder+filename+".csv"):
  inscope_sfdc_info_df=pd.read_csv(sfdc_text_folder+filename+".csv",encoding="ISO-8859-1")
print("processing file length - ",len(inscope_sfdc_info_df))
#Connecting SQL db to get SFDC data
sql_cursor = SQL_connection("server","database","username","password")
cursor=sql_cursor.cursor()
output_df=pd.DataFrame()
status=''

def concurrent_function(cvalue):
  try:
    global output_df
    global inscope_sfdc_info_df
#     print("scipe",len(inscope_sfdc_info_df))
    item=cvalue[1]
    category_type=cvalue[0]
    subct=cvalue[2]
    indx=cvalue[3]
    specid=cvalue[4]
#     print("specid",specid)
    org_value=str(item)   
    value=org_value.strip().lower()
    ignore_data=[]
    edit_inscope_sfdc=inscope_sfdc_info_df.copy()
    #validating required sfdc column 
    for validate in sfdc_validate_column:
      try:
        if value.isdigit() and len(value)>0 :  
          digit_value_list=[]
          if value[0]=="0":        
            digit_value_list.append(value)
            digit_value_list.append(int(value))
          else:
            digit_value_list.append(value)
          for int_value in digit_value_list:
            rgx = re.compile(r'((?<!lsr)(?<!silsoft)(?<!\d)(^|\s+|#|\(){}([^-\d]|$))'.format(int_value),re.IGNORECASE)  
            re_match=inscope_sfdc_info_df[inscope_sfdc_info_df[validate].str.contains(rgx,na=False)] 
            if len(re_match)>0: 
                for index, row in re_match.iterrows():
                  try:
                    emailid=row["EmailId"]
                    inscope_sfdc_info_df.drop(inscope_sfdc_info_df[inscope_sfdc_info_df["EmailId"]==emailid].index,inplace=True)
                    validate_str=row[validate]
                    result=rgx.search(validate_str)
                    if result:
                      index_search=result.start()
                      starting_index=index_search-20
                      if starting_index<0:
                        starting_index=0
                      ending_index=index_search+len(result.group())+20
                      if ending_index>len(validate_str):
                        ending_index=len(validate_str)
                      matched_str=validate_str[starting_index:ending_index]
                    else:
                      matched_str=''
                    re_match.loc[index,"MatchedSFDCColumn"]=validate
                    re_match.loc[index,"MatchedSFDCValue"]=matched_str
                    for item in skip_list:
                      if item in matched_str.lower():
                        re_match.drop(re_match[re_match["EmailId"]==emailid].index,inplace=True)
                        break
                  except Exception as e:
                    status=output_str+" --> Oops error found in looping sfdc validate column"+str(e)
                digit_match_row=adding_matched_values(re_match,category_type,indx,org_value,subct,specid)
                output_df=pd.concat([output_df,digit_match_row])
        elif len(value)>0 and ("?" not in value and "!" not in value):    
          trim_value=value.replace("silopren*",'')
          e_value=trim_value.replace("*",'')
          e_value=e_value.replace("®",'')
          whole_match=pd.DataFrame()
          w_rgx = re.compile(r"(([^a-zA-Z]|^){}([^a-zA-Z]|$))".format(e_value),re.I)
          whole_match=inscope_sfdc_info_df[inscope_sfdc_info_df[validate].str.contains(w_rgx,na=False)]    
          if len(whole_match)>0:
            for index, row in whole_match.iterrows():
                try:
                  emailid=row["EmailId"]
                  inscope_sfdc_info_df.drop(inscope_sfdc_info_df[inscope_sfdc_info_df["EmailId"]==emailid].index,inplace=True)
                  validate_str=row[validate]
                  result=w_rgx.search(validate_str)
                  if result:
                      index_search=result.start()
                      starting_index=index_search-20
                      if starting_index<0:
                        starting_index=0
                      ending_index=index_search+len(result.group())+20
                      if ending_index>len(validate_str):
                        ending_index=len(validate_str)
                      matched_str=validate_str[starting_index:ending_index]
                  else:
                    matched_str=''
                  whole_match.loc[index,"MatchedSFDCColumn"]=validate
                  whole_match.loc[index,"MatchedSFDCValue"]=matched_str              
                except Exception as e:
                  status=output_str+" --> Oops error found in processing"+str(e)
            string_match_column=adding_matched_values(whole_match,category_type,indx,org_value,subct,specid)
            output_df=pd.concat([output_df,string_match_column])
      except Exception as e: 
        status=output_str+" --> Oops error found in looping sfdc validate column"+str(e)
  except Exception as e:
    status=output_str+" --> Oops error found in looping sfdc validate column"+str(e)

try:
  if len(inscope_sfdc_info_df)>0:
    concurrent_function(cvalue)
    # inserting into sfdc indentified table
    if len(output_df)>0:
#       print("result")
#       print(output_df)
      output_df.drop_duplicates(inplace=True)
      output_df=output_df[(sfdc_column+adding_custom_column)]
      output_df=output_df.fillna("NULL")
      output_df=output_df.replace({"None":"NULL"})
      cursor=sql_cursor.cursor()
      output_list = output_df.values.tolist()
      for row in output_list:
        try:            
          insert_data=''
          for item in row:
            item=str(item)
            if "'" in item:
              item=item.replace("'","''")
            insert_data+="'"+item+"',"
          if len(insert_data)>0:
            insert_data=insert_data+"'NULL','NULL'"
#             print("insert",insert_data)
#             print("query",sfdc_insert_query)
            insert_query=sfdc_insert_query+insert_data+")"
            cursor.execute(insert_query)
            sql_cursor.commit()
          status=output_str+" --> "+str(len(output_list))+" case detail(s) found"
        except Exception as e:
          status=output_str+" --> Oops error found while inserting"+str(e)
          dbutils.notebook.exit(status)      
    else:
      status=output_str+" --> 0 case detail found"

except Exception as e:
  status=output_str+" --> Oops error found in processing"+str(e)
  dbutils.notebook.exit(status)
  
dbutils.notebook.exit(status)


# COMMAND ----------

