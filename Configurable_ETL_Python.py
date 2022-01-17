import boto3
import decimal
import numpy as np
import json
import pandas as pd
import s3fs
import re
import yaml
import io
from ast import literal_eval
from decimal import Decimal
from boto3.dynamodb.conditions import Key,Attr

session = boto3.Session()
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
s3 = s3fs.S3FileSystem(anon=False)

Retention_list = ['subject_death','ltfu_date','last_contact_date','last_contact_type','subther_start_date','all_ipdc_date']

##### Reading Configuration details of country_site analyte ################
def read_config (file_name):
    #config_general = pd.read_excel(f's3://csv-lambda-dynamodb-bucket/Config_files/'+file_name,sheet_name="General")
    config_get_data = pd.read_excel(f's3://csv-lambda-dynamodb-bucket/Config_files/'+file_name,sheet_name="GET_DATA")
    config_df_operations = pd.read_excel(f's3://csv-lambda-dynamodb-bucket/Config_files/'+file_name,sheet_name="DF_OPERATIONS")
    return [config_get_data,config_df_operations]

retention = ['subject_death']

def reading_source_data(primkey,srtkey):
    response = get_dynamodb_resource().Table('AllSources_Table').query(
        ProjectionExpression="#st, #vie, #dat",
        ExpressionAttributeNames={"#st" : "study_code","#vie" : "view","#dat" : "data"},
        KeyConditionExpression=Key('study_code').eq(primkey) & Key('view').eq(srtkey)
        )
    df = pd.json_normalize(
        response['Items'], 
        record_path =['data'], 
        meta=['study_code', 'view']
        )
    return df

def columns_selection(df,cols,dist_flg):
    col_list = cols.split(',')
    try:
        if dist_flg == 'Y':
            df = df[col_list]
            df = df.drop_duplicates(subset = col_list)
        else:
            df = df[col_list]  
    except KeyError:
        pass
    return df

def extracting_sitenumber(df,col):
    pattern = re.compile(r"^.{3,4}(?=-)")
    df[df[col].str.contains(pattern)==True]

def str_to_bool(s):
    if s.upper() == 'TRUE':
         return True
    elif s.upper() == 'FALSE':
         return False
    else:
         raise ValueError('BOOLEAN OPERAND invalid: ' + s)

def groupby_slice (df, condition_str):
    condition_lst = [i.strip() for i in condition_str.split(',')]
    group_by_cols_lst = [i.strip() for i in condition_lst[0].split('=')]
    group_by_cols = group_by_cols_lst[1].split(',') # finalize group by columns in case we have a list separated by ,
    slice_num_lst = [i.strip() for i in condition_lst[1].split('=')]
    slice_num = int(slice_num_lst[1])
    res = df.groupby(group_by_cols).apply(lambda x: x.iloc[:slice_num])
    return res.droplevel(0).reset_index()

def sort_dataset (df, condition_str):
    ascending_order=True #by default we do ascending sort
    condition_lst = [i.strip() for i in condition_str.split(',')]
    
    if len(condition_lst)>1: # if length of condition it means sort order is provided
        sort_order_lst = [i.strip() for i in condition_lst[1].split('=')]
        sort_order = sort_order_lst[1]
        if sort_order.upper() == 'DESC':
            ascending_order=False

    condition = [i.strip() for i in condition_lst[0].split('=')]
    init = iter(condition)
    res_dct = dict(zip(init, init))
    sort_cols=res_dct.get('sort_cols').split(',')
    res = df.sort_values(by=sort_cols, ascending=ascending_order)
    return res
    
def extract_date (in_str):
    date_time_str = in_str.upper().replace('DATE(', '').replace(')', '')
    date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d')
    return date_time_obj

def extract_number (in_str):
    num_str = in_str.upper().replace('NUMBER(', '').replace(')', '')
    num = float(num_str)
    return num

def extract_boolean (in_str):
    bool_str = in_str.upper().replace('BOOLEAN(', '').replace(')', '')
    bool_val = str_to_bool(bool_str)
    return bool_val

def handling_filter_operation(df,filters):
    filter_op = None
    if "&" in filters:
        df_new = filter_two_parts(df, filters, '&')
    elif "|" in filters:
        df_new = filter_two_parts(df, filters, '|')
    #check if it is BETWEEN operation
    elif "BETWEEN" in filters.upper():
        df_new = filter_between(df, filters, 'BETWEEN')
    else:
        if 'NOT NULL' in filters:
            df_new = filter_single_part(df, filters, '=')
        #check if it is = or >= or <=
        elif ">=" in filters:
            df_new = filter_single_part(df, filters, '>=')
        elif "<=" in filters:
            df_new = filter_single_part(df, filters, '<=')
        elif "=" in filters:
            df_new = filter_single_part(df, filters, '=')
        else:
            raise ValueError('NON-EXISTING FILTER OPERAND encountered: ' + filters)
    return df_new

def filter_between(df, filters, operand):
    left_col_lst= [i.strip() for i in filters.split(operand)]
    col_name = left_col_lst[0]
    #remove 1st and last character which are brackets
    limits = left_col_lst[1][1:-1]
    limits_lst = [i.strip() for i in limits.split(',')]
    if limits_lst[0].upper().startswith('DATE'):
        date_obj = extract_date(limits_lst[0])
        left_col_val = date_obj
    elif limits_lst[0].upper().startswith('NUMBER'):
        num = extract_number(limits_lst[0])
        left_col_val = num
    else:
        left_col_val=limits_lst[0]
    if limits_lst[1].upper().startswith('DATE'):
        date_obj = extract_date(limits_lst[1])
        right_col_val = date_obj
    elif limits_lst[1].upper().startswith('NUMBER'):
        num = extract_number(limits_lst[1])
        right_col_val = num
    else:
        right_col_val=limits_lst[1]
    df_new = df[(df[col_name]>=left_col_val)&(df[col_name]<=right_col_val)]
    return df_new                                   
    
    
def filter_single_part(df, filters, operand):
    left_col_lst= [i.strip() for i in filters.split(operand)]
    left_col = left_col_lst[0]
    if 'NOT NULL' in left_col_lst[1].upper():
        df_new = removing_null(df,left_col)
        return df_new
    #check if value is DATE()
    elif left_col_lst[1].upper().startswith('DATE'):
        date_obj = extract_date(left_col_lst[1])
        left_col_val = date_obj
    elif left_col_lst[1].upper().startswith('NUMBER'):
        num = extract_number(left_col_lst[1])
        left_col_val = num
    elif left_col_lst[1].upper().startswith('BOOLEAN'):
        bool_val = extract_boolean(left_col_lst[1])
        left_col_val = bool_val
    else:
        left_col_val=left_col_lst[1]
    if operand =='=':
        df_new = df[df[left_col]==left_col_val]
    elif operand =='>=':
        df_new = df[df[left_col]>=left_col_val]
    elif operand =='<=':
        df_new = df[df[left_col]<=left_col_val]
    else:
        raise ValueError('NON-EXISTING FILTER OPERAND encountered: ' + operand)
    return df_new

def filter_two_parts(df, filters, operand):
    # perform component extraction for AND operation
    filters_parts_lst = [i.strip() for i in filters.split(operand)]    
    left_col_lst= [i.strip() for i in filters_parts_lst[0].split('=')]
    left_col = left_col_lst[0]
    left_col_val=left_col_lst[1]
    right_col_lst= [i.strip() for i in filters_parts_lst[1].split('=')]
    right_col = right_col_lst[0]
    right_col_val=right_col_lst[1]
    if operand =='&':
        df_new = df[(df[left_col]==left_col_val)&(df[right_col]==right_col_val)]
    elif operand =='|':
        df_new = df[(df[left_col]==left_col_val)|(df[right_col]==right_col_val)]
    else:
        raise ValueError('NON-EXISTING FILTER OPERAND encountered: ' + operand)
    return df_new
    
def handling_filter_operation_OLD(df,filters):
    lis = filters.split("&")
    for x in lis:
        if ("null" or "Null") in x:
            not_null_col = x.split("is")[0].strip()
            df = removing_null(df,not_null_col)
        elif ("=") in x:
            con = x.replace("=","==")
            df = filter_rows(df,con)
        else:
            df = filter_rows(df,x)
    return df

def removing_null(df,col):
    df.dropna(subset=[col],inplace=True)
    return df
    
def filter_rows(df,condition):
    df = df.query(condition)
    return df

def rename_columns(df,condition):
    condition = [i.strip() for i in condition.split('=')]
    init = iter(condition)  
    res_dct = dict(zip(init, init))
    df_new = df.rename(columns = res_dct)
    return df_new

def casting_columns(df,condition):
    condition_lst = condition.split(',')
    column_name_lst = condition_lst[0].split('=')
    type_lst = condition_lst[1].split('=')

    if '#' in type_lst[1]: # this means that we have included data pattern
        format_lst = type_lst[1].split('#')
        column_type = format_lst[0].strip()
        format_pattern = format_lst[1].strip()
    else: # no data pattern included
        column_type = type_lst[1]
    column_name = column_name_lst[1].strip()

    if column_type.strip().upper()== 'DATETIME':
        
        df[column_name] = pd.to_datetime(df[column_name], format=format_pattern)
    elif column_type.strip().upper()=='NUMBER':
        df[column_name] = df[column_name].apply(pd.to_numeric)
    return df
    
def joining_columns(df1,df2,condition,dataframe,join_type):
    lis = []
    for i in condition.split('&'):
        d = {}
        for j in i.split('='):
            d[j.split('.')[0].strip()] = j.split('.')[1].strip()
        lis.append(d)
    df_list = [i.replace("'","").strip() for i in dataframe.split(',')]
    left_on,right_on = [],[]
    
    for i in lis:
        left_on.append(i[df_list[0].replace("'","").strip()])
        right_on.append(i[df_list[1].replace("'","").strip()])
    df = pd.merge(df1,df2,left_on=left_on,right_on=right_on,how=join_type)
    return df
    
def aggregate_operation(df,condition):
    lis = []
    for x in condition.split(','):
        dic = {}
        dic[x.split('=')[0].strip()] = x.split('=')[1].replace("'","").strip()
        lis.append(dic)
    dic = {k: v for d in lis for k, v in d.items()}
    
    input_column = dic['new_column']
    operation = dic['operation']
    affect_cols = dic['operation_cols'].split('#')
    
    if operation.upper() == 'MINIMUM':
        df[input_column] = df[affect_cols].min(axis=1)
    
    if operation.upper() == 'MAXIMUM':
        df[input_column] = df[affect_cols].max(axis=1)
        
    return df
    
def removing_columns(df,condition):
    drop_cols = [i.strip() for i in condition.split(',')]
    df.drop(drop_cols, axis='columns', inplace=True)
    return df

def unique_columns(df,condition):
    uniq_cols = condition.split(',')
    df = df.drop_duplicates(subset = uniq_cols)
    return df
    
def get_dynamodb_resource():
    dynamodb = boto3.resource("dynamodb",region_name = "us-east-1")
    return dynamodb

def get_nested_rec(s):
    rec = {}
    rec['study_code'] = s['studycode']
    rec['subject_hierarchy'] = s['subject_hierarchy']
    rec['country'] = s['country']
    rec['site'] = s['site']
    rec['subject'] = s['subject']
    rec['Retention'] = dict(s[Retention_list])
    return rec

def reading_analyte_data(obj_name,cols):
    cols_list = cols.strip().split(",")
    response = get_dynamodb_resource().Table('standardized_table_universal').scan()
    df = pd.json_normalize(
        response['Items']
        )
  
    globals()[str(obj_name)] = df[cols_list]

def get_existing_analyte(obj_name):
    df = globals()[obj_name.replace("'","").strip()]
    return df
    
def add_column(df,condition):
    lis = []
    for x in condition.split(','):
        dic = {}
        dic[x.split('=')[0].strip()] = x.split('=')[1].replace("'","").strip()
        lis.append(dic)
    dic = {k: v for d in lis for k, v in d.items()}
    
    new_col = dic['new_column']
    value = dic['value']
    
    df[new_col] = value
    return df

def bind_rows (df_names_str):
    df_lst = []
    df_names_lst = [i.strip() for i in df_names_str.split(',')]
    for df_name in df_names_lst:
        df = globals()[df_name]
        df_lst.append(df)
    result = pd.concat(df_lst, axis = 0, sort=False)
    return result
    
def groupby_summarise (df, condition):
    condition = [i.strip() for i in condition.split('|')] # separate main parameters by |
    group_by_cols_lst=condition[0].split('=') # take groupby columns as list separated by =
    group_by_cols = group_by_cols_lst[1].split(',') # finalize group by columns in case we have a list separated by ,
    summary_col_ops_lst = condition[1].split('=') # process summary operations
    condition1 = [i.strip() for i in summary_col_ops_lst[1].split(',')]
    res_dct ={} # define dictionary for summary operations
    for c in condition1:
        c_lst = c.split('#') # separate into list by #
        res_dct[c_lst[0]] = c_lst[1] # load to dictionary each part as key and value
    res1 = df.groupby(group_by_cols).agg(res_dct) # perform groupby with summary operations
    return res1.reset_index()

def remove_rows (df,condition):
    condition_lst= [i.strip() for i in condition.split('=')]
    column_name = condition_lst[0]
    operation = condition_lst[1]
    if operation.upper() == 'NULL1':
        df = df.dropna(subset=[column_name])
    elif operation.upper() == 'NULL':
        df = df[df[column_name].notna()]
    else:
        raise ValueError('NON-EXISTING REMOVE ROWS OPERAND encountered: ' + condition)
    return df

#decision_table implementation
def get_decision_table_as_df():
    data = {'CXCHERAD':['Yes', 'No'],
            'output':[True, False]}
    # Create DataFrame
    df = pd.DataFrame(data)
    return df

#decision_table implementation
def assign_cond_value(df, new_col, lookup_col, new_val, lookup_val):
    df.loc[df[lookup_col] == lookup_val, new_col] = new_val
    return df

#decision_table implementation
def assign_cond_value_operation(df, condition, file_name):
    condition_lst = [i.strip() for i in condition.split(',')]
    new_col_lst = [i.strip() for i in condition_lst[0].split('=')]
    new_col = new_col_lst[1]
    lookup_col_lst = [i.strip() for i in condition_lst[1].split('=')]
    lookup_col = lookup_col_lst[1]
    tbl_name_lst = [i.strip() for i in condition_lst[2].split('=')]
    tbl_name = tbl_name_lst[1]
    #dec_tbl_df = get_decision_table_as_df() #read table from word sheet using tbl_name
    dec_tbl_df = read_config_sheet (file_name, tbl_name)
    print(dec_tbl_df)
    for index, row in dec_tbl_df.iterrows():
        df = assign_cond_value(df, new_col, lookup_col, row['output'], row[lookup_col])
    return df

def select_columns(df, condition):
    condition_lst = [i.strip() for i in condition.split(',')]
    return df[condition_lst]
    
def select_distinct_rows_for_columns(df, condition):
    condition_lst = [i.strip() for i in condition.split(',')]
    df_new = df.drop_duplicates(subset = condition_lst).reset_index()
    return df_new[condition_lst]

def attach_column (df, condition):
    condition_lst = [i.strip() for i in condition.split(',')]
    column_name_lst = [i.strip() for i in condition_lst[0].split('=')]
    source_col_lst = [i.strip() for i in condition_lst[1].split('=')]
    operation_lst = [i.strip() for i in condition_lst[2].split('=')]
    column_name = column_name_lst[1]
    source_col = source_col_lst[1]
    operation = operation_lst[1]
    # check if we have more than 3 main parameters

    if len(condition_lst) > 3:
        value_lst = [i.strip() for i in condition_lst[3].split('=')]
        column_value_lst = [i.strip() for i in condition_lst[4].split('=')]
        value = value_lst[1]
        column_value = column_value_lst[1]  
    
    if operation.upper() == 'NOTNULL':
        df[column_name] = df[source_col].notnull()
    if operation.upper() == 'SUMEQ':
        source_col_lst = [i.strip() for i in source_col.split('|')]
        df[column_name] = df[source_col_lst[0]].sum() == df[source_col_lst[1]].sum()
    if operation.upper() == 'NULL':
        df[column_name] = np.where(df[source_col].isnull(), value, df[column_value])
    return df
    
def get_data(df):
    view_names_list = []
    for i,j in df.iterrows():
        cols = j['Column']
        fil = j['Filter']
        dist = j['Distinct']
        data_src = j['Data Source']
        obj_name = j['Object Name'].strip()
        if data_src == 'Analyte':
            #reading_analyte_data(obj_name,cols) #used if we would read analyte from DB for every update
            get_existing_analyte(obj_name)
            
        else:
            globals()[str(j['View'])] = reading_source_data(j['study_code'],j['View'])
            if pd.isna(fil) == True:
                pass
            else:
                globals()[str(j['View'])] = handling_filter_operation(globals()[str(j['View'])],fil)
            globals()[str(j['View'])] = columns_selection(globals()[str(j['View'])],cols,dist)
            view_names_list.append(str(j['View']))
    return view_names_list
    
def perform_operations(df, config_file_name):
    for i,j in df.iterrows():
        type_op = j['Type']
        dataframe = j['Data Frames'].strip()
        condition = j['Condition']
        if type_op.upper() == 'RENAME COLUMN':
            globals()[str(j['Object Name']).replace("'","").strip()] = rename_columns(globals()[str(j['Data Frames']).replace("'","").strip()],condition)
        elif type_op.upper() == 'FORMAT COLUMN':
            df_format = casting_columns(globals()[str(j['Data Frames']).replace("'","").strip()],condition)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_format
        elif type_op.upper() == 'FILTER':
            df_filter = handling_filter_operation(globals()[str(j['Data Frames']).replace("'","").strip()],condition)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_filter
        elif type_op.upper() == 'LEFT JOIN':
            df_list = dataframe.split(',')
            df1 = globals()[str(df_list[0]).replace("'","").strip()]
            df2 = globals()[str(df_list[1]).replace("'","").strip()]
            df_join=any_join(df1,df2, 'left', condition)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_join
        elif type_op.upper() == 'OUTER JOIN':
            df_list = dataframe.split(',')
            df1 = globals()[str(df_list[0]).replace("'","").strip()]
            df2 = globals()[str(df_list[1]).replace("'","").strip()]
            df_join=any_join(df1,df2, 'outer', condition)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_join
        elif type_op.upper() == 'AGGREGATE COLUMN':
            df_aggregate_column = aggregate_operation(globals()[str(j['Data Frames']).replace("'","").strip()],condition)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_aggregate_column
        elif type_op.upper() == 'REMOVE COLUMN':
            globals()[str(j['Object Name']).replace("'","").strip()] = removing_columns(globals()[str(j['Data Frames']).replace("'","").strip()],condition)
        elif type_op.upper() == 'UNIQUE COLUMN':
            globals()[str(j['Object Name']).replace("'","").strip()] = unique_columns(globals()[str(j['Data Frames']).replace("'","").strip()],condition)
        elif type_op.upper() == 'ADD COLUMN':
            df_add_column = add_column(globals()[str(j['Data Frames']).replace("'","").strip()],condition)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_add_column
        elif type_op.upper() == 'BIND ROWS':
            df_bind_rows = bind_rows(dataframe)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_bind_rows
        elif type_op.upper() == 'SORT DATASET':
            df_sort_dataset = sort_dataset (globals()[dataframe],condition)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_sort_dataset
        elif type_op.upper() == 'GROUPBY SUMMARISE':
            df_group_summarise = groupby_summarise (globals()[dataframe],condition)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_group_summarise
        elif type_op.upper() == 'REMOVE ROWS':
            df_remove_rows = remove_rows (globals()[dataframe],condition)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_remove_rows
        elif type_op.upper() == 'DECISION COLUMN':
            df_decision_column = assign_cond_value_operation (globals()[dataframe],condition, config_file_name)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_decision_column
        elif type_op.upper() == 'SELECT COLUMNS':
            df_select_columns = select_columns (globals()[dataframe],condition)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_select_columns
        elif type_op.upper() == 'SELECT DISTINCT':
            df_select_distinct_column = select_distinct_rows_for_columns (globals()[dataframe],condition)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_select_distinct_column
        elif type_op.upper() == 'ATTACH COLUMN':
            df_attach_column = attach_column (globals()[dataframe],condition)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_attach_column
        elif type_op.upper() == 'GROUPBY SLICE':
            df_groupby_slice = groupby_slice (globals()[dataframe],condition)
            globals()[str(j['Object Name']).replace("'","").strip()] = df_groupby_slice
        else: 
            raise ValueError('NON-EXISTING DF_OPERATION encountered: ' + type_op.upper())
        # Raise an error if we do not get a hit of the operations

    return None
    
def reading_country_site_data():
    response = get_dynamodb_resource().Table('standardized_table').scan()
    df = pd.json_normalize(
        response['Items']
        )
    globals()[str('country_site_subject')] = df[['country','site','subject']]
    
def any_join (left, right, join_type, condition_str):
    condition_lst = condition_str.split(',')
    result = pd.merge(left, right, how=join_type, on=condition_lst)
    return result

    
def converting_standardized_form(df,study_global):
    df = df.replace({pd.NaT: 'Null'})
    df = df.replace({np.NaN: 'Null'})
    
    all_columns = list(df)
    df[all_columns] = df[all_columns].astype('string')
    
    df['studycode'] = study_global
    df['subject_hierarchy'] = df['country'].map(str)+ '#' + df['site'].map(str) + '#' + df['subject'].map(str)
   
    records = []
    
    for i,j in df.iterrows():
        rec = get_nested_rec(j)
        records.append(rec)
        
    #print(records)
    
    ddb_data = json.loads(json.dumps(records), parse_float=Decimal)
    
    #print(ddb_data)
    
    table = get_dynamodb_resource().Table("standardized_table")
    
    for x in ddb_data:
        study_code = x['study_code']
        subject_hierarchy = x['subject_hierarchy']
        country = x['country']
        site = x['site']
        subject = x['subject']
        Retention = x['Retention']
        
        table.put_item(
            Item={
                'study_code' : study_code,
                'subject_hierarchy' : subject_hierarchy,
                'country' : country,
                'site' : site,
                'subject' : subject,
                'Retention': Retention
            }
        )

def derive_analyte (analyte_name, config_file_name):
    # Step 1 - Read Config file
    config_dfs = read_config(config_file_name)
    # Step 2 - Get Data
    view_names_list = get_data(config_dfs[0])
    # Step 3 - Perform Operations
    perform_operations(config_dfs[1], config_file_name)
    return globals()[analyte_name] # should be made to return analyte object
    
def process_study (study_analytes_df):
    print('process_study -  called.')
    # For each record in study_analytes_df call derive_analyte
    complete_analyte_df = pd.DataFrame() #start with empty data frame
    for index, row in study_analytes_df.iterrows():
        print('Deriving Analyte: ' + row['Analyte'])
        if row['Analyte']=='country_site_subject':
            globals()[str(['study_global'])] = row['study_code']
            complete_analyte_df = derive_analyte(row['Analyte'], row['Config'])
        else:
            analyte_df = derive_analyte(row['Analyte'], row['Config'])
            complete_analyte_df = any_join(complete_analyte_df,analyte_df, 'left', 'subject')
        
        print(complete_analyte_df)
        
    return complete_analyte_df
    
def read_study_config (file_name):
    config_general = pd.read_excel(f's3://csv-lambda-dynamodb-bucket/Config_files/'+file_name,sheet_name="General")
    return config_general

def read_config_sheet (file_name, worksheet_name):
    df = pd.read_excel(f's3://csv-lambda-dynamodb-bucket/Config_files/'+file_name,sheet_name=worksheet_name)
    return df
    
if __name__ == '__main__':
    study_config_df = read_study_config('study_configuration.xlsx')
    final_df = process_study(study_config_df)
    study_global = globals()[str(['study_global'])]
    converting_standardized_form(final_df,study_global)
