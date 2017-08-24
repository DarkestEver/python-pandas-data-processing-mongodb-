# -*- coding: utf-8 -*-
"""
Created on Fri Aug 25 02:39:12 2017

@author: myself
"""
import os
import re            
import json
import pymongo

#mongdb connection
client = MongoClient('mongodb://localhost:27017/')
db = client['stakoverflow']

#get all files from directory
def get_filepaths(directory):
    file_paths = [] 
    for root, directories, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)
    return file_paths

# rename df column by removing special char from dataframe and insert to mongodb
def df_to_mongodb(df_data):    
    ls_df_header = list(df_data) # read dataframe column 
    #removing special char and number from list
    ls_new_df_header = []
    for str_df_header in ls_df_header:    
        ls_new_df_header.append("".join(re.findall("[a-zA-Z]+", str_df_header)))
    df_mapping.columns = ls_new_df_header #rename column by removing special character
    data_json = df_mapping.to_dict('records') #convert dataframe to json
    #import to mongodb         
    db.test.insert_many(data_json)
    client.close()
            
# file filtering 
def file_filtering(df_data,filename,sheetname=''):
    ls_c=[]
    ls_col = list(df_data.columns)           
    len_ls_col = len(ls_col)
    if len_ls_col >  3 :
        ls_filesht = [filename, sheetname,'ok']
        ls_filesht.extend(ls_col)
        ls_c.extend(ls_filesht)
    else:
        ls_c.extend([filename,sheetname,'not valid'] )
    return ls_c

def ls_to_df_row(ls_data,start):
    ls_transpose = []
    ls_len= len(ls_data)
    if ls_len > start:
        for x in range(3,len(ls_data)):
            ls_transpose.append([ls_data[x]])
    return ls_transpose         
                
def start_proc(folder_path):
    # call function to get all files into list
    #"E:\\keshav docs\\OneDrive\\Projects\\sample data\\OneDrive_1_7-2-2017\\01 - Excel"
    full_file_paths = get_filepaths(folder_path)
    # Processing filename and sheet name(in case of excel)
    for filename in full_file_paths:   
        if filename.lower().endswith(('.xls', '.xlsx')) == True:
            xl_fil = pd.ExcelFile(filename)
            sheetnames = xl_fil.sheet_names
            for shtname in sheetnames:
                df = xl_fil.parse(shtname)
               
        elif filename.lower().endswith(('.csv')) == True:
        
        else:
       
