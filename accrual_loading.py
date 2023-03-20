import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
import json
import pandas as pd
import sqlalchemy as sd
import boto3
import urllib3
import os
import datetime
import numpy as np
from dateutil.parser import parse

error_msg = list()
success_msg = list()

def lambda_handler(event, context):
    global error_msg
    global success_msg
    error_msg.clear()
    success_msg.clear()
    key_list = []
    for record in event['Records']:
        key_list.append(record['s3']['object']['key'])
    file_key = key_list[0]
    s3_client = boto3.client("s3")
    ssm = boto3.client("ssm")
    failure = ssm.get_parameter(Name="failure_hook_url", WithDecryption=True).get("Parameter").get("Value")
    success = ssm.get_parameter(Name="success_hook_url", WithDecryption=True).get("Parameter").get("Value")
    http = urllib3.PoolManager()
    host_client = ssm.get_parameter(Name="db_host", WithDecryption=True).get("Parameter").get("Value")
    user_name = ssm.get_parameter(Name="lambda_db_username", WithDecryption=True).get("Parameter").get("Value")
    user_password =ssm.get_parameter(Name="lambda_db_password", WithDecryption=True).get("Parameter").get("Value")
    file_dbname = "seronetdb-Vaccine_Response"
    sql_column_df, engine, conn = connect_to_sql_db(host_client, user_name, user_password, file_dbname)
    accrual_loader_main(sql_column_df, engine, conn, s3_client,file_key)
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def connect_to_sql_db(host_client, user_name, user_password, file_dbname):

    sql_column_df = pd.DataFrame(columns=["Table_Name", "Column_Name", "Var_Type", "Primary_Key", "Autoincrement",
                                          "Foreign_Key_Table", "Foreign_Key_Column"])
    creds = {'usr': user_name, 'pwd': user_password, 'hst': host_client, "prt": 3306, 'dbn': file_dbname}
    connstr = "mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{dbn}"
    engine = sd.create_engine(connstr.format(**creds))
    engine = engine.execution_options(autocommit=False)
    conn = engine.connect()
    metadata = sd.MetaData()
    metadata.reflect(engine)

    for t in metadata.tables:
        try:
            curr_table = metadata.tables[t]
            curr_table = curr_table.columns.values()
            for curr_row in range(len(curr_table)):
                curr_dict = {"Table_Name": t, "Column_Name": str(curr_table[curr_row].name),
                             "Var_Type": str(curr_table[curr_row].type),
                             "Primary_Key": str(curr_table[curr_row].primary_key),
                             "Autoincrement": False,
                             "Foreign_Key_Count": 0,
                             "Foreign_Key_Table": 'None',
                             "Foreign_Key_Column": 'None'}
                curr_dict["Foreign_Key_Count"] = len(curr_table[curr_row].foreign_keys)
                if curr_table[curr_row].autoincrement is True:
                    curr_dict["Autoincrement"] = True
                if len(curr_table[curr_row].foreign_keys) == 1:
                    key_relation = list(curr_table[curr_row].foreign_keys)[0].target_fullname
                    key_relation = key_relation.split(".")
                    curr_dict["Foreign_Key_Table"] = key_relation[0]
                    curr_dict["Foreign_Key_Column"] = key_relation[1]


                sql_column_df = pd.concat([sql_column_df, pd.DataFrame.from_records([curr_dict])])
        except Exception as e:
            print(e)
    print("## Sucessfully Connected to " + file_dbname + " ##")
    sql_column_df.reset_index(inplace=True, drop=True)
    return sql_column_df, engine, conn


def accrual_loader_main(sql_column_df, engine, conn, s3_client, file_key):
    
    #sql_column_df, engine, conn = connect_to_sql_db(pd, sd, file_dbname)
    
    sub_folder = "Monthly_Accrual_Reports"
    
    #get file from bucket and rename columns if necessary
    bucket_name = 'seronet-trigger-submissions-passed'
    
    all_submissions = []
    cbc_code = []
    all_submissions, cbc_code = get_all_submissions(s3_client, bucket_name, sub_folder, "Feinstein_CBC01", 41, all_submissions, cbc_code)
    all_submissions, cbc_code = get_all_submissions(s3_client, bucket_name, sub_folder, "UMN_CBC02", 27, all_submissions, cbc_code)
    all_submissions, cbc_code = get_all_submissions(s3_client, bucket_name, sub_folder, "ASU_CBC03", 32, all_submissions, cbc_code)
    all_submissions, cbc_code = get_all_submissions(s3_client, bucket_name, sub_folder, "Mt_Sinai_CBC04", 14, all_submissions, cbc_code)
    file_key = os.path.dirname(file_key)
    all_submissions = [i for i in all_submissions if file_key in i]
    print(all_submissions)
    
    #
    for submission in all_submissions:
        print(f'Start transforming {submission}')

        acc_participant_data_key = os.path.join(submission, 'Accrual_Participant_Info.csv')
        obj = s3_client.get_object(Bucket=bucket_name, Key= acc_participant_data_key)
        acc_participant_data = pd.read_csv(obj['Body'], na_filter = False)
        print('start uploading data for acc_participant_data')
        acc_participant_data = acc_participant_data.replace("Sunday_Prior_To_Visit_1", "Week_Of_Visit_1")
        acc_participant_data.rename(columns = { "Week_Of_Visit_1": "Sunday_Prior_To_Visit_1"}, inplace = True)
        upload_data(acc_participant_data, "Accrual_Participant_Info", engine, conn, [])

        acc_visit_data_key = os.path.join(submission, 'Accrual_Visit_Info.csv')
        obj = s3_client.get_object(Bucket=bucket_name, Key= acc_visit_data_key)
        acc_visit_data = pd.read_csv(obj['Body'], na_filter = False)
        print('start uploading data for acc_visit_data')
        acc_visit_data.rename(columns={'Collected_in_This_Reporting_Period': 'Collected_In_This_Reporting_Period'}, inplace=True)
        acc_visit_data.replace("Baseline(1)", 1, inplace=True)
        upload_data(acc_visit_data, "Accrual_Visit_Info", engine, conn, ["Visit_Number"])
    
        acc_vaccine_data_key = os.path.join(submission, 'Accrual_Vaccination_Status.csv')
        obj = s3_client.get_object(Bucket=bucket_name, Key= acc_vaccine_data_key)
        acc_vaccine_data = pd.read_csv(obj['Body'], na_filter = False)
        print('start uploading data for acc_vaccine_data')
        acc_vaccine_data.rename(columns={'Visit_Date_Duration_From_Visit_1': 'SARS-CoV-2_Vaccination_Date_Duration_From_Visit1'}, inplace=True)
        acc_vaccine_data.replace("Baseline(1)", 1, inplace=True)
        upload_data(acc_vaccine_data, "Accrual_Vaccination_Status", engine, conn, ["Visit_Number", "Vaccination_Status", "SARS-CoV-2_Vaccine_Type"])

        
        

def upload_data(data_table, table_name, engine, conn, primary_col):
    sql_df = pd.read_sql(f"Select * FROM {table_name}", conn)
    sql_type_df = pd.read_sql(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table_name}'", conn)
    sql_df.fillna("N/A", inplace=True)
    '''
    if table_name == "Accrual_Vaccination_Status":
        s3 = boto3.client('s3')
        bucket_name = 'seronet-trigger-submissions-passed'
        key = 'Accrual_Vaccination_Status.csv'
        csv_buffer = data_table.to_csv(index=False).encode()
        s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer)
    if table_name == "Accrual_Visit_Info":
        s3 = boto3.client('s3')
        bucket_name = 'seronet-trigger-submissions-passed'
        key = 'Accrual_Visit_Info.csv'
        csv_buffer = data_table.to_csv(index=False).encode()
        s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer)
    if table_name == "Accrual_Participant_Info":
        s3 = boto3.client('s3')
        bucket_name = 'seronet-trigger-submissions-passed'
        key = 'Accrual_Participant_Info.csv'
        csv_buffer = data_table.to_csv(index=False).encode()
        s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer)
    '''
    for curr_col in sql_df:
        sql_df[curr_col] = [convert_data_type(c) for c in sql_df[curr_col]]

    for curr_col in sql_df.columns:
        if curr_col in ['Site_Cohort_Name', 'Primary_Cohort', 'Serum_Volume_For_FNL', 'Num_PBMC_Vials_For_FNL', 'PBMC_Concentration']:  #numeric but N/A allowed
            sql_df[curr_col] = [str(c) for c in sql_df[curr_col]]
            
    for curr_col in data_table.columns:
        if curr_col in ['Site_Cohort_Name']:
            data_table[curr_col] = [str(c) for c in data_table[curr_col]]
        else:
            data_table[curr_col] = [convert_data_type(c) for c in data_table[curr_col]]
        if curr_col in ['Site_Cohort_Name', 'Primary_Cohort', 'Serum_Volume_For_FNL', 'Num_PBMC_Vials_For_FNL', 'PBMC_Concentration']:  #numeric but N/A allowed
            data_table[curr_col] = [str(c) for c in data_table[curr_col]]
    if "Sunday_Prior_To_Visit_1" in data_table.columns:
        data_table["Sunday_Prior_To_Visit_1"] = [i.date() if isinstance(i, datetime.datetime) else i for i in data_table["Sunday_Prior_To_Visit_1"]]

    if "Visit_Date_Duration_From_Visit_1" in data_table.columns:
        data_table["Visit_Date_Duration_From_Visit_1"] = data_table["Visit_Date_Duration_From_Visit_1"].replace("\.0", "", regex=True)
        
    if "PBMC_Concentration" in sql_df.columns and "Num_PBMC_Vials_For_FNL" in sql_df.columns:
        sql_df["PBMC_Concentration"] = sql_df["PBMC_Concentration"].replace("\.0", "", regex=True)
        sql_df["Num_PBMC_Vials_For_FNL"] = sql_df["Num_PBMC_Vials_For_FNL"].replace("\.0", "", regex=True)
    
    
    
    data_table_dtypes = data_table.dtypes
    sql_df_dtypes = sql_df.dtypes
    '''
    if table_name == 'Accrual_Participant_Info' and 'Comments' in data_table.keys():
        data_table['Comments'] = data_table['Comments'].astype(sql_df_dtypes['Comments'])
    if table_name == 'Accrual_Visit_Info':
        if len(sql_df) == 0:
            for key in data_table.keys():
                if data_table_dtypes[key] != sql_df_dtypes[key]:
                    sql_df[key] = sql_df[key].astype(data_table_dtypes[key])
        else:
            for key in data_table.keys():
                if data_table_dtypes[key] != sql_df_dtypes[key]:
                    #data_table[key] = data_table[key].astype(sql_df_dtypes[key])
                    
    '''
    if table_name == 'Accrual_Visit_Info':
        for key in data_table.keys():
            if data_table_dtypes[key] != sql_df_dtypes[key]:
                sql_df[key] = sql_df[key].astype(data_table_dtypes[key])

    primary_keys = ["Research_Participant_ID"] + primary_col
    data_table = data_table.drop_duplicates(subset=primary_keys, keep="last") # avoid when re-upload the same data, the previous droped values got updated to the DB
    check_data = data_table.merge(sql_df, how="left", indicator="first_pass")
    check_data = check_data.query("first_pass == 'left_only'")
   
    
    check_data = check_data.merge(sql_df[primary_keys], how="left", on=primary_keys, indicator="second_pass")
    new_data = check_data.query("second_pass == 'left_only'")   # primary keys do not exist
    update_data = check_data.query("second_pass == 'both'")     # primary keys exist but data update

    if not new_data is None:
        new_data.drop(["first_pass", "second_pass"], axis=1, inplace=True)
        if not new_data is None:
            new_data = new_data.drop_duplicates(subset=primary_keys, keep="last")

    if not update_data is None:
        update_data.drop(["first_pass", "second_pass"], axis=1, inplace=True)
        if not update_data is None:
             update_data = update_data.drop_duplicates(subset=primary_keys, keep="last")
    
    try:
        if not new_data is None:
            if len(new_data) > 0:
                for col in new_data.keys():
                    if sql_type_df.loc[sql_type_df['COLUMN_NAME'] == col, 'DATA_TYPE'].iloc[0] == 'float':
                        new_data[col] = new_data[col].replace(['N/A'], np.nan)
                new_data.to_sql(name=table_name, con=engine, if_exists="append", index=False)
                conn.connection.commit()
                print(f"there are {len(new_data)} records to be added for table {table_name}")
    except Exception as e:
        print(e)
    if not update_data is None:
        if len(update_data) > 0:
            for col in update_data.keys():
                if sql_type_df.loc[sql_type_df['COLUMN_NAME'] == col, 'DATA_TYPE'].iloc[0] == 'float':
                    update_data[col] = update_data[col].replace(['N/A'], np.nan)
            update_tables(conn, engine, primary_keys, update_data, table_name)
            conn.connection.commit()


def update_tables(conn, engine, primary_keys, update_table, sql_table):
    key_str = ['`' + str(s) + '`' + " like '%s'" for s in primary_keys]
    key_str = " and ".join(key_str)

    col_list = update_table.columns.tolist()
    col_list = [i for i in col_list if i not in primary_keys]

    print(f"there are {len(update_table)} records to be udpated for table {sql_table}")
    for index in update_table.index:
        try:
            curr_data = update_table.loc[index, col_list].values.tolist()
            primary_value = update_table.loc[index, primary_keys].values.tolist()
            update_str = ["`" + i + "` = '" + str(j) + "'" for i, j in zip(col_list, curr_data)]
            update_str = ', '.join(update_str)
            sql_query = (f"UPDATE {sql_table} set {update_str} where {key_str %tuple(primary_value)}")
            print(sql_query)
            engine.execute(sql_query)
        except Exception as e:
            print(e)
        finally:
            conn.connection.commit()


def get_all_submissions(s3_client, bucket_name, sub_folder, cbc_name, cbc_id, all_submissions, cbc_code):
    """ scans the buceket name and provides a list of all files paths found """
    uni_submissions = []
    Prefix=sub_folder + "/" + cbc_name
    try:
        key_list = s3_client.list_objects(Bucket=bucket_name, Prefix=sub_folder + "/" + cbc_name)
        if 'Contents' in key_list:
            key_list = key_list["Contents"]
            key_list = [i["Key"] for i in key_list if ("UnZipped_Files" in i["Key"])]
            file_parts = [os.path.split(i)[0] for i in key_list]
            file_parts = [i for i in file_parts if "test/" not in i[0:5]]
            file_parts = [i for i in file_parts if "Submissions_in_Review" not in i]
            uni_submissions = list(set(file_parts))
        else:
            uni_submissions = []  # no submissions found for given cbc
    except Exception as e:
        print(e)
    finally:
        cbc_code = cbc_code + [str(cbc_id)]*len(uni_submissions)
        return all_submissions + uni_submissions, cbc_code


def convert_data_type(v):
    if isinstance(v, (datetime.datetime, datetime.time, datetime.date)):
        return v
    if str(v).find('_') > 0:
        return v
    try:
        float(v)
        if (float(v) * 10) % 10 == 0:
            return int(float(v))
        return float(v)
    except ValueError:
        try:
            v = parse(v)
            return v
        except ValueError:
            return v
        except TypeError:
            str(v)

def write_to_slack(message_slack, slack_chanel):
    http = urllib3.PoolManager()
    data={"text": message_slack}
    r=http.request("POST", slack_chanel, body=json.dumps(data), headers={"Content-Type":"application/json"})
