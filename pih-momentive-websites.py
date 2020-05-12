# Databricks notebook source
# Databricks notebook source
# Databricks notebook source
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 10:07:14 2019

@author: 809917
"""
#************************************
#In[1]: importing Required packages
#************************************
import configparser
import camelot
import urllib.request as req
from urllib.request import urlopen, URLError, HTTPError
from dateutil import parser as date_parser
import json
import logging
from bs4 import BeautifulSoup
import shutil
import os
import pandas as pd
import datetime
import glob
import fitz
import pyodbc
import datetime
import cv2
import numpy as np
import pytesseract
from wand.image import Image
import re

#************************************
#Loging environment setup
#************************************

logger = logging.getLogger('momentive')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("momentive_web.log", 'w')
fh.setLevel(logging.DEBUG)
ch = logging.FileHandler("momentive_web_error.log", 'w')
ch.setLevel(logging.ERROR)
formatter =logging.Formatter(fmt = '%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)

#********************************************
#Declaring global variables for web source:
#********************************************
web_prod_list = []
metadata_list =[]
html_list_pd=[]
file_exist_audit_check =[]

#****************************************************************************************
#Getting the input and output file path from config file
#****************************************************************************************
config = configparser.ConfigParser()
config.read("/dbfs/mnt/momentive-configuration/config-file.ini")
momen_url = config.get('website', 'web_url').split(',')
products =  config.get('website', 'web_products').split(',')
web_out_path = config.get('website', 'web_out_path')
metadata_outpath = config.get('website', 'metadata_outpath')
silicone_elast_path = config.get('website', 'silicone_elast_path')

#****************************************************************************************
#Function Name : web_incremental_check
#Ojective: checking the product metadata with audit control table data based 
#on timestamp or fil_length to process the product file again or not:
#****************************************************************************************
def web_incremental_check(file,web_inc_flag,content_info,web_df):
  try:
    if not web_df.empty:        
        web_file_filt_df = web_df[web_df['file_name'] == file]
        if not web_file_filt_df.empty:
          file_exist_audit_check.append(file)
          for web_index in web_file_filt_df.index: 
            if content_info.get('Last-Modified') != None:
              if web_file_filt_df['updated'][web_index] != 'Not Available':  
                if date_parser.parse(web_file_filt_df['updated'][web_index])  < date_parser.parse(content_info.get('Last-Modified')):
                  logger.info('{} existing file but it has been updated in the website, So processing it again'.format(file))
                  web_inc_flag = 's'
              else:
                if int(web_file_filt_df['file_size'][web_index]) < int(content_info.get('Content-Length')):
                  logger.info('{} existing file but it has been updated in the website, So processing it again'.format(file))
                  web_inc_flag = 's'
            else:
                if int(web_file_filt_df['file_size'][web_index]) < int(content_info.get('Content-Length')):
                  logger.info('{} existing file but it has been updated in the website, So processing it again'.format(file))
                  web_inc_flag = 's'
        else:
          logger.info('{} new file from the website'.format(file))
          web_inc_flag = 's'
    else:
        logger.info('{} new file from the website'.format(file))
        web_inc_flag = 's'
    if web_inc_flag == '':
      logger.info('{} existing file but no changes done on the file, So we are using existing data'. format(file))
    return web_inc_flag
  except Exception as e:
    logger.error('Something went wrong in the web_incremental_check function', exc_info=True)

#***************************************************************************************************************
#Function Name : web_duplicat_file_check
#Ojective : checking only the unique files are processed filtering done based on the existing processed files 
#***************************************************************************************************************
def web_duplicat_file_check(file,write_flag,page_url,content_info):
  try:
    global metadata_list
    if page_url not in web_prod_list and len(metadata_list) !=0:
        meta_df = pd.DataFrame(metadata_list)
        meta_df_parse = meta_df[meta_df[0]==file]
        if not meta_df_parse.empty:
            for meta_index in meta_df_parse.index:
                if meta_df_parse[2][meta_index] != 'Not Available' and content_info.get('Last-Modified') != None:
                    if date_parser.parse(meta_df_parse[2][meta_index]) < date_parser.parse(content_info.get('Last-Modified')):  
                        meta_df_parse = meta_df[~(meta_df[0]==file)]
                        metadata_list = meta_df_parse.values.tolist()
                        write_flag = 's'
                else:
                    if meta_df_parse[1][meta_index] < content_info.get('Content-Length'):
                        meta_df_parse = meta_df[~(meta_df[0]==file)]
                        metadata_list = meta_df_parse.values.tolist()
                        write_flag = 's'    
        else:
            write_flag = 's'
    elif page_url in web_prod_list:
        logger.info('{} this file has been processed already'.format(file))
        write_flag = ''
    else:
        write_flag = 's'      
    return write_flag
  except Exception as e:
    logger.error('Something went wrong in the web_duplicat_file_check function', exc_info=True)

#***************************************************************************************************************
#Function Name : web_file_write
#writing the web files to the staging area for validation
#***************************************************************************************************************

def web_file_write(content_info,file,web_raw_path,content,mode,file_website_path):
  try:
    global metadata_list
    #writing pdf file into raw 
    with open(web_raw_path + file, mode) as f:
        f.write(content)
        f.close()
        logger.info('{} has been successfully copied from momentive website to the raw blob storage {}'        
        .format(file,web_out_path))               
    file_meta_info =[]    
    if content_info.get('Content-Length') != None:
        file_length = content_info.get('Content-Length')

    if content_info.get('Last-Modified') != None:
        file_date = content_info.get('Last-Modified')
    else:
      file_date = 'Not Available'   
    file_meta_info.append(file)
    file_meta_info.append(file_length) 
    file_meta_info.append(file_date)
    file_meta_info.append(file_website_path)
    metadata_list.append(file_meta_info)
    return metadata_list
  except Exception as e:
    logger.error('Something went wrong in the web_file_write function', exc_info=True)

#*******************************************************************************************************************************
#Function Name : Product_category
#Extracting the usage of product based on the categories identified using TaxonName from the momentive home page metadata json
#*******************************************************************************************************************************
def Product_category(index,web_out_path,web_df):
    try:
        write_flag = ''
        web_inc_flag = ''
        if index['PageUrl'].strip().lower().startswith('/en-us/'):
            response = urlopen('https://www.momentive.com'+ index['PageUrl'])
        else:
            response = urlopen(index['PageUrl'])
        content_info = response.info()
        logger.info('{} file-info : Last-Modified is {} and file-size is {} we will update this details on unstructure audit table'\
                      .format(index['PageTitle'].strip().replace('/', '-'), content_info.get('Last-Modified'), content_info.get('Content-Length')))
        
        file = index['PageTitle'].strip().replace('/', '-') + '.txt'
        web_inc_flag = web_incremental_check(file[:-4],web_inc_flag,content_info,web_df)
        page_url = 'https://www.momentive.com'+ index['PageUrl']
        if web_inc_flag == 's':
          write_flag = web_duplicat_file_check(file,write_flag,page_url,content_info)
        if write_flag == 's':
          web_prod_list.append(page_url)
          content = response.read()
          soup = BeautifulSoup(content, 'lxml')
          divTag = soup.find_all("div", {"class": "text-white-sm pad-sm-4-4 pad-md-3-3 contain-sm"})
          content = ''
          for tag in divTag:
              tdTags = tag.find_all("p")
              for tag in tdTags:
                  content = content + tag.text + '\n'
          file_write = web_file_write(content_info,file,web_out_path,content,'w')
    except Exception as e:
      logger.error('Something went wrong in the Product_category function', exc_info=True) 
      
#*******************************************************************************************************************************
#Function Name : Product_Extract          
#Extracting the files for each product based on the metadata json from momentive home page for web source:    
#*******************************************************************************************************************************
def Product_Extract(index,web_out_path,web_df):
    try:
        if '?' in index['PageUrl'].split('/')[-1]:
          write_flag = ''
          web_inc_flag = ''
          response = urlopen(index['PageUrl'])
          content_info = response.info()
          #index['Last-Modified'] = content_info.get('Last-Modified')
          #index['Content-Length'] = content_info.get('Content-Length')
          logger.info('{} file-info : Last-Modified is {} and file-size is {} we will update this details on unstructure audit table'\
                      .format(index['PageTitle'].strip().replace('/', '-'), content_info.get('Last-Modified'), content_info.get('Content-Length')))
          if content_info.get('Content-Type') != None:
            file_extension = content_info.get('Content-Type').split('/')[-1]
          else:
            file_extension = 'pdf'
          file = index['PageTitle'].strip().replace('/', '-') + '.' + file_extension
          web_inc_flag = web_incremental_check(file[:-4],web_inc_flag,content_info,web_df)
          page_url = index['PageUrl']
          if web_inc_flag == 's':
            write_flag = web_duplicat_file_check(file,write_flag,page_url,content_info)
          if write_flag == 's':
              web_prod_list.append(page_url)
              content = response.read()
              file_write = web_file_write(content_info,file,web_out_path,content,'wb',index['PageUrl'])
        else:
            #Product_category(index,web_out_path,web_df)
            response = urlopen(index['PageUrl'])
            content_info = response.info()
            content_type = content_info.get('Content-Type').split('/')[-1]
            logger.info('{} its a {} so it cannot be processed'.format(index['PageUrl'],content_type))
            html_list=[]
            html_list.append(index['PageUrl'])
            html_list.append(content_type)
            html_list_pd.append(html_list)
                    
    except Exception as e:
      logger.error('Something went wrong in the Product_Extract function', exc_info=True)
    finally:
        df1 = pd.DataFrame(metadata_list,columns=['File_name', 'File_size','Date','website_path'])

#*******************************************************************************************************************************
#Function Name : Related_doc
#products bind in the related documents list from the momentive home page 
#*******************************************************************************************************************************
def Related_doc(index,web_out_path,web_df):
    try: 
        if index["RelatedDocuments"] is not None and index["RelatedDocuments"] != []:
            for relate_doc in index["RelatedDocuments"]:   
              if '?' in relate_doc['DocumentUrl'].split('/')[-1]:
                  write_flag = ''
                  web_inc_flag = ''
                  response = urlopen(relate_doc['DocumentUrl'])
                  content_info = response.info()
                  #relate_doc['Last-Modified'] = content_info.get('Last-Modified')
                  #relate_doc['Content-Length'] = content_info.get('Content-Length')
                  logger.info('{} file-info : Last-Modified is {} and file-size is {} we will update this details on unstructure audit table'\
                   .format(relate_doc['DocumentTitle'].strip().replace('/', '-') , content_info.get('Last-Modified'), content_info.get('Content-Length')))
                  if content_info.get('Content-Type') != None:
                    file_extension = content_info.get('Content-Type').split('/')[-1]
                  else:
                    file_extension = 'pdf'
                  file = relate_doc['DocumentTitle'].strip().replace('/', '-') + '.' + file_extension
                  web_inc_flag = web_incremental_check(file[:-4],web_inc_flag,content_info,web_df)
                  page_url = relate_doc['DocumentUrl']
                  if web_inc_flag == 's':
                    write_flag = web_duplicat_file_check(file,write_flag,page_url,content_info,)
                  if write_flag == 's':
                      web_prod_list.append(page_url)
                      content = response.read()
                      file_write = web_file_write(content_info,file,web_out_path,content,'wb',relate_doc['DocumentUrl'])
    except Exception as e:
      logger.error('Something went wrong in the Related_doc function ', exc_info=True)
      
#**********************************************************************   
#Function name : path_exists
#Ojvective :  Creating the empty folder for website Source
#**********************************************************************
def path_exists(file_path):
  try:
    if file_path is not None and file_path != '':
      dbutils.fs.rm(file_path.replace("/dbfs",""),True)
      dbutils.fs.mkdirs(file_path.replace("/dbfs","dbfs:"))
      logger.info('Successfully created website blob stoarge {} '.format(file_path))
    else:
      logger.error('Website output is None or empty ')    
  except Exception as e:
      logger.error('Something went wrong in the path_exists function ', exc_info=True) 
      
#**********************************************************************  
#Function name : Control_table_website_check
#Ojvective :  Fetching the website data from sql for incremental load
#**********************************************************************
def Control_table_website_check(sql_conn):
  try:
    cursor = sql_conn.cursor()
#     alter_query  = 'ALTER TABLE momentive.website_control_table ALTER COLUMN website_path varchar(265)'
#     cursor.execute(alter_query)
    sql_conn.commit()
    if sql_conn is not None: 
      select_query = config.get('website','website_data')
      web_df = pd.read_sql(select_query, sql_conn)
      logger.info('Successfully extracted the data for website from sql server')
    else:
      logger.error('Sql_conn has None value something went wrong in the Sql server connection')
      web_df = pd.DataFrame([], columns=['source_type', 'file_name', 'file_type', 'created', 'updated', 'file_size'])
    return web_df
  except Exception as error:
    logger.error('Something went wrong in the Control_table_website_check function', exc_info=True)
    
#*************************************************
#Function name : Sql_db_connection
#Ojective  : connecting Azure sql db using pyodbc
#*************************************************
def Sql_db_connection(): 
  try:
    server = config.get('sql_db', 'server')
    database = config.get('sql_db', 'database')
    username = config.get('sql_db', 'username')
    password = config.get('sql_db', 'password')
    
    DATABASE_CONFIG = {
      'server': server,
      'database': database,
      'username': username,
      'password': password
    }
    driver= "{ODBC Driver 17 for SQL Server}"
    connection_string = 'DRIVER=' + driver + \
                      ';SERVER=' + DATABASE_CONFIG['server'] + \
                      ';PORT=1433' + \
                      ';DATABASE=' + DATABASE_CONFIG['database'] + \
                      ';UID=' + DATABASE_CONFIG['username'] + \
                      ';PWD=' + DATABASE_CONFIG['password'] 


    sql_conn = pyodbc.connect(connection_string)
    logger.info('Successfully connected with the sql serevr ')
    return sql_conn    
  except Exception as e:
    logger.error('Something went wrong in the Sql_db_connection function {} ', exc_info=True) 
    
#Writing metadata json for each product
def metadata_outpath_write(metadata_outpath,product,results,search_area):
    if search_area == 'TDS':
      with open(metadata_outpath + product+ '_TDS' + '.json','w') as js:
                json.dump(results,js)
    else:
      with open(metadata_outpath + product+ '.json','w') as js:
                json.dump(results,js)

#metadata json extraction for each product categpries from momentive home page  
def web_home(web_out_path,web_df):
    try:      
        for product in products:
            logger.info('Extracting the metadata json for {} from momentive Home page(www.momentive.com/en-us) '\
                        .format(product))
            url_handle = urlopen(momen_url[0].format(product))
            headers = url_handle.read().decode("utf-8")
            soup = BeautifulSoup(headers, 'lxml')
            divTag = soup.find_all("div", {"class": "app-content-area inner-wrapper"})
            tdTags = divTag[0].find_all(id="Model")
            headers = json.loads(tdTags[0].text)
            results = headers['Results']
            logger.info('{} related products found in the momentive home page for {}'.format(len(results),product))
            for index in results:
                if index['PageUrl'] is not None:
                    if index['PageUrl'].strip().lower().startswith('https'):
                        if index['TaxonName'] is not None:
                           # Product_category(index,web_out_path,web_df)
                            Related_doc(index,web_out_path,web_df)
                        else:
                            Product_Extract(index,web_out_path,web_df)
                            
                    elif index['PageUrl'].strip().lower().startswith('/en-us/'):
                        if index['TaxonName'] is not None:
                            #Product_category(index,web_out_path,web_df)
                            Related_doc(index,web_out_path,web_df)
                        else:
                            Product_Extract(index,web_out_path,web_df)
                else:
                    if index['TaxonName'] is not None:
                      Related_doc(index,web_out_path,web_df)
            metadata_outpath_write(metadata_outpath,product,results,'home')              
        logger.info('Successfully Completed Fetching all files from Momentive Home Page')  
        
    except Exception as e:
      logger.error('Something went wrong in the web_home {} ', exc_info=True) 
      
#metadata json extraction for each product categpries from TDS page      
def tds_web(web_out_path,web_df):
  try:
    #base_dir = web_out_path.replace('dbfs:','/dbfs') + 'TDS/'
    #path_exists(base_dir)
    base_dir = web_out_path.replace('dbfs:','/dbfs')
    for product in products:
        logger.info('Extracting the metadata json for {} from momentive TDS page(www.momentive.com/en-us/tdssearch)'\
                        .format(product))
        url_handle = urlopen(momen_url[1].format(product))
        headers = url_handle.read().decode("utf-8")
        soup = BeautifulSoup(headers, 'lxml')
        divTag = soup.find_all("div", {"class": "app-content-area inner-wrapper"})
        tdTags = divTag[0].find_all(id="Model")
        headers = json.loads(tdTags[0].text)
        results = headers['Results']
        logger.info('{} related products found in the momentive TDS page for {}'.format(len(results),product))
        for index in results:
            write_flag = ''
            web_inc_flag = ''                                            
            if index['PageUrl'] is not None:
              Product_Extract(index,base_dir,web_df)              
        metadata_outpath_write(metadata_outpath,product,results,'TDS')
  except Exception as e:
    logger.error('Something went wrong in the tds_web {} ', exc_info=True) 

#performing data insertion on unstructured control table:
def web_sql_crud_control_table(sql_conn,web_df):
  try:
    cursor = sql_conn.cursor()
    global metadata_list
    metadata_list = pd.DataFrame(metadata_list,columns=['File_name', 'File_size','Date','website_path'])
    
    #creating data in the conatrol audit table for historical load
    if web_df.empty:
      for i in metadata_list.index:
        insert_query = config.get('website', 'web_data_insert').format(metadata_list['File_name'][i][:-4], metadata_list['File_name'][i][-3:], metadata_list['Date'][i], metadata_list['Date'][i], metadata_list['File_size'][i], metadata_list['website_path'][i])
        cursor.execute(insert_query)
        sql_conn.commit()
        logger.info('Successfully inserted the data into unstructred audit table for {}'.format(metadata_list['File_name'][i][:-4]))
    #CRUD operation in the control audit table for incremental load
    
    else:
      for i in metadata_list.index:
        incriment_filt_df = web_df[(web_df['file_name']==metadata_list['File_name'][i][:-4])]       
        #Create query 
        if incriment_filt_df.empty:
          insert_query = config.get('website', 'web_data_insert').format(metadata_list['File_name'][i][:-4], metadata_list['File_name'][i][-3:], 
                        metadata_list['Date'][i], metadata_list['Date'][i], metadata_list['File_size'][i], metadata_list['website_path'][i])
          cursor.execute(insert_query)
          sql_conn.commit()
          logger.info('Successfully inserted the data into unstructred audit table for {}'.format(metadata_list['File_name'][i][:-4]))
        #Update operation
        else:
          update_query = config.get('website', 'web_data_update').format(metadata_list['Date'][i], metadata_list['File_size'][i], 
                                                                         metadata_list['File_name'][i][:-4], 'Website')
          cursor.execute(update_query)
          sql_conn.commit()
          logger.info('Successfully updated the data into unstructred audit table for {}'.format(metadata_list['File_name'][i][:-4]))
      #Delete operations:
      if bool(file_exist_audit_check):
        file_exist_audit_check_set = set(file_exist_audit_check)
        file_list = set(web_df['file_name'].to_list())
        file_difference = list(file_list.difference(file_exist_audit_check_set))
        for file_name in file_difference:
          delete_query = config.get('website', 'web_data_delete').format('Website',file_name)
          cursor.execute(delete_query)
          sql_conn.commit()
          logger.info('Successfully deleted the data into unstructred audit table for {}'.format(file_name))
  except Exception as error:
    logger.error('Something went wrong in the web_sql_crud_control_table', exc_info=True) 

def web_pdf_extract_text(path, nativeloc, allfiles):
    try:
        allfiles = config.get(path,allfiles)
        path_exists(allfiles)
        native_files = glob.glob(config.get(path, nativeloc) + '*.pdf')
        for files in native_files:
            text=''
            pdf_file = fitz.open(files)
            n_pages = pdf_file.pageCount
            for n in range(n_pages):
                page = pdf_file.loadPage(n)
                text = text + page.getText()            
            basenames=files.split('/')        
            basenames = allfiles + basenames[-1].split('.')[0]
            text_name = basenames.replace("/dbfs","dbfs:") + '.txt'
            dbutils.fs.put(text_name,text,True)
    except Exception as e:
      logger.error(e)

#************************************
#Website file extraction
#************************************
if __name__ == '__main__':     
    try:
      logger.info('Beginning of file extraction from momentive websites')
      path_exists(web_out_path)
      path_exists(metadata_outpath)
      sql_conn = Sql_db_connection()
      web_df = Control_table_website_check(sql_conn)
      web_home(web_out_path,web_df)
      tds_web(web_out_path,web_df)
      web_sql_crud_control_table(sql_conn,web_df)
    except Exception as e:
      logger.error('Something went wrong in the main ',exc_info=True)
    finally:
      df1 = pd.DataFrame(metadata_list,columns=['File_name', 'File_size','Date'])


# COMMAND ----------

# MAGIC %sh
# MAGIC cat /databricks/driver/momentive_web_error.log

# COMMAND ----------



# COMMAND ----------

