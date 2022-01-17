import json
import os
import pandas as pd
import numpy as np
import boto3
import s3fs
import re
from decimal import Decimal
from datetime import datetime

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
s3 = s3fs.S3FileSystem(anon=False)

config_bucket = 'dev-data-lake-ds-raw-eu-west-1-239126490696'
config_key = 'Config_files/config.json'

def get_dynamodb_resource():
    dynamodb = boto3.resource("dynamodb",region_name = "eu-west-1")
    return dynamodb
    
def handling_different_file_formats(bucket,key):
    file_details = os.path.splitext(key)
    file_extension = file_details[1].replace('.','')
    
    if file_extension == 'sas7bdat':
        data = pd.read_sas(f's3://{bucket}/{key}',format='sas7bdat',encoding='utf-8')
        data['Folder'] = 'SAS'
    elif file_extension == 'csv':
        data = pd.read_csv(f's3://{bucket}/{key}')
    else:
        print("Different file formats are received. Hence code will be failed")
        
    return data
    
def appending_view_name_to_data(df,view_id):
    df['Folder'] = view_id
    return df
    
def appending_view_name_to_data_clinical_view(df,view_name,view_id):
    df[view_name] = view_id
    return df
      
def ecoa_header_data_preparation(key):
    a = key.split('/')[-1]
    edited_string = a.split('_')
    study = edited_string[0]
    globals()[str(['view_id_ecoa'])] = edited_string[1] + edited_string[2]
    #view_id = edited_string[1] + edited_string[2]
    source_timestamp = edited_string[3].split('.')[0]
    now = datetime.now()
    load_timestamp = now.strftime("%Y%m%d%H%M")
    header_data = [{'source_timestamp' : source_timestamp, 'load_timestamp' : load_timestamp, 'Study' : study }]
    return header_data
    
def sas_header_data_preparation(bucket,key):
    a1 = key.split('/')[-1]
    edited_string1 = a1.split('_')
    source_timestamp_sas = edited_string1[1].split('.')[0]
    now = datetime.now()
    load_timestamp_sas = now.strftime("%Y%m%d%H%M")
    a2 = key.split('/')
    file_type_sas = a2[3]
    globals()[str(['view_id_sas'])] = edited_string1[0].upper()+"#"+a2[2]
    folder_path = a2[0]+"/"+a2[1]+"/"+a2[2]+"/"+a2[3]+"/"+a2[4]+"/"+a2[5]+"/"+a2[6]+"/"
    s3_location = bucket+"/"+folder_path
    header_data = [{'source_timestamp' : source_timestamp_sas, 'load_timestamp' : load_timestamp_sas, 'File_Type' : file_type_sas, 's3_location' : s3_location}]
    return header_data
    
def Clinical_view_header_data_preparation(data,bucket,key):
    a1 = key.split('/')[-1]
    edited_string1 = a1.split('_')
    src_timestamp1 = edited_string1[5]
    src_timestamp2 = edited_string1[6].split('.')[0]
    source_timestamp_clinical_view = src_timestamp1 + src_timestamp2
    now = datetime.now()
    load_timestamp_clinical_view = now.strftime("%Y%m%d%H%M%S")
    s3_location = bucket + '/' + key
    view_id_1 = edited_string1[2] + '_' + edited_string1[3] + '_' + edited_string1[4]
    view_id_clinical_view = view_id_1.split('.')[1]

    Folder = data['Folder'].unique()
    out_arr_Folder = np.array_str(Folder)
    Folder = out_arr_Folder.replace("'", "")
    Folder = Folder.replace("[", "")
    Folder = Folder.replace("]", "")

    Folder_name = data['FolderName'].unique()
    out_arr_Folder_name = np.array_str(Folder_name)
    Folder_name = out_arr_Folder.replace("'", "")
    Folder_name = Folder_name.replace("[", "")
    Folder_name = Folder_name.replace("]", "")

    globals()[str(['view_id_vendor_shortname_CV'])] = Folder + "#" + 'Rave'
    #view_id_vendor_shortname_clinical_view = globals()[str(['view_id_vendor_shortname'])]
    
    header_data = [{'Source_timestamp' : source_timestamp_clinical_view, 'load_timestamp' : load_timestamp_clinical_view, 'Folder Name' : Folder_name, 'View' : view_id_clinical_view, 'S3 Location' : s3_location}]
    
    return header_data
    
def final_dataframe_creation(data,data1,key_columns,table_columns):
    item_list = [e for e in data.columns if e not in (key_columns[0],key_columns[1])]
    
    for i,j in data.iterrows():
        data1 = pd.concat([pd.DataFrame([[j[key_columns[0]],j[key_columns[1]],dict(j[item_list])]], columns=data1.columns), data1], ignore_index=True)
        
    final_df = data1.groupby([table_columns[0],table_columns[1]])[table_columns[2]].apply(list).reset_index()
    
    return final_df
    
def writing_to_dynamodb(proc,final_df):
    records = []
    
    for i,j in final_df.iterrows():
        records.append(j.to_dict())
        
    print(records)
    
    ddb_data = json.loads(json.dumps(records), parse_float=Decimal)
    
    print(ddb_data)
    
    table = get_dynamodb_resource().Table("dev-ofr-datahub-cms-clinical-study-consolidation")
    
    if proc == 'ecoa':
        for x in ddb_data:
            study_code = x['study_code']
            view = x['view']
            header = x['header']
            data = x['data']
            
            table.put_item(
                Item={
                    'studycode' : study_code,
                    'viewID#vendorshortname' : view,
                    'header' : header,
                    'data' : data
                    }
                )
    elif proc == 'SAS':
        for x in ddb_data:
            study_code = x['study_code']
            view = x['view']
            header = x['header']
            data = x['data']
            
            table.put_item(
                Item={
                    'studycode' : study_code,
                    'viewID#vendorshortname' : view,
                    'header' : header,
                    'data' : data
                    }
                )
    elif proc == 'Clinical_View':
        for x in ddb_data:
            study_code = x['study_code']
            view = x['view']
            header = x['header']
            data = x['data']
            
            table.put_item(
                Item={
                    'studycode' : study_code,
                    'viewID#vendorshortname' : view,
                    'header' : header,
                    'data' : data
                    }
                )
    elif proc == 'SSOT':
        for x in ddb_data:
            study_code = x['study_code']
            view = x['view']
            data = x['data']
            
            table.put_item(
                Item={
                    'studycode' : study_code,
                    'viewID#vendorshortname' : view,
                    'data' : data
                    }
                )
    else:
            study_code = x['study_code']
            view = x['view']
            data = x['data']
            
            table.put_item(
                Item={
                    'studycode' : study_code,
                    'viewID#vendorshortname' : view,
                    'data' : data
                    }
                )
        
if __name__ == '__main__':
    print(event)
    test_string = event['Records'][0]['body']
    res = json.loads(test_string)
    test_string_1 = res['Message']
    res1 = json.loads(test_string_1)
    bucket = res1['Records'][0]['s3']['bucket']['name']
    key = res1['Records'][0]['s3']['object']['key']
    
    print(bucket)
    print(key)
    
    with s3.open(f's3://{config_bucket}/{config_key}', 'r') as json_file:
        config_dic = json.load(json_file,parse_float=Decimal)
        
    table_columns = config_dic['Target_table_columns']
    view_names_list = config_dic['view_names_list']
    folder_na_list = config_dic['folder_na_list']
    
    result = [x for x in view_names_list if x.upper() in key.upper()]
    str1 = " "
    proc = str1.join(result)
    
    if proc == 'AE':
        proc = 'Clinical_view'
        key_columns = config_dic['source_files'][proc]['key columns']
    elif 'SAS' in proc:
        proc = 'SAS'
        key_columns = config_dic['source_files'][proc]['key columns']
    elif 'CLINICALVIEWS' in proc:
        proc = 'Clinical_View'
        key_columns = config_dic['source_files'][proc]['key columns']
    else:
        key_columns = config_dic['source_files'][proc]['key columns']
        
    data = handling_different_file_formats(bucket,key)
    data.fillna(value="Null",inplace=True)
        
    #if proc in folder_na_list:
        #data['Folder'] = proc
        #data["Study"].fillna(method ='ffill', inplace = True)
    
    if proc == 'ecoa':
        data1 = pd.DataFrame(columns=[table_columns[0],table_columns[1],table_columns[2]],dtype=object)
        header_data = ecoa_header_data_preparation(key)
        view_id_ecoa = globals()[str(['view_id_ecoa'])]
        view_id_data = appending_view_name_to_data(data,view_id_ecoa)
        view_id_data.sort_values([key_columns[0],key_columns[1]],inplace=True)
        final_df = final_dataframe_creation(view_id_data,data1,key_columns,table_columns)
        final_df['header'] = header_data
        writing_to_dynamodb(proc,final_df)
    elif proc == 'SAS':
        data1 = pd.DataFrame(columns=[table_columns[0],table_columns[1],table_columns[2]],dtype=object)
        header_data = sas_header_data_preparation(bucket,key)
        view_id_sas = globals()[str(['view_id_sas'])]
        view_id_data = appending_view_name_to_data(data,view_id_sas)
        view_id_data.sort_values([key_columns[0],key_columns[1]],inplace=True)
        final_df = final_dataframe_creation(view_id_data,data1,key_columns,table_columns)
        final_df['header'] = header_data
        writing_to_dynamodb(proc,final_df)
    elif proc == 'Clinical_View':
        data1 = pd.DataFrame(columns=[table_columns[0],table_columns[1],table_columns[2]],dtype=object)
        header_data = Clinical_view_header_data_preparation(data,bucket,key)
        view_id_clinical_view = globals()[str(['view_id_vendor_shortname_CV'])]
        view_id_data = appending_view_name_to_data_clinical_view(data,key_columns[1],view_id_clinical_view)
        view_id_data.sort_values([key_columns[0],key_columns[1]],inplace=True)
        final_df = final_dataframe_creation(view_id_data,data1,key_columns,table_columns)
        final_df['header'] = header_data
        writing_to_dynamodb(proc,final_df)
    elif proc == 'SSOT':
        data1 = pd.DataFrame(columns=[table_columns[0],table_columns[1],table_columns[2]],dtype=object)
        data.sort_values([key_columns[0],key_columns[1]],inplace=True)
        data1 = pd.DataFrame(columns=[table_columns[0],table_columns[1],table_columns[2]],dtype=object)
        final_df = final_dataframe_creation(data,data1,key_columns,table_columns)
        writing_to_dynamodb(proc,final_df)
    else:
        data.sort_values([key_columns[0],key_columns[1]],inplace=True)
        data1 = pd.DataFrame(columns=[table_columns[0],table_columns[1],table_columns[2]],dtype=object)
        final_df = final_dataframe_creation(data,data1,key_columns)
        writing_to_dynamodb(proc,final_df)