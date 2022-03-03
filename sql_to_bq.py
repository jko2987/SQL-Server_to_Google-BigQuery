import os
import ast
import sys
import datetime
import pyodbc
import pandas as pd
import pandas_gbq as gbq
import json
from google.auth import jwt
from google.cloud import pubsub_v1
from google.oauth2 import service_account
import socket
import re

#*********************************************************************************************

#Any Computer that installs this program needs ODBC Driver 17 for SQL Server driver installed

    # LINK TO DOWNLOAD Microsoft ODBC 17 for SQL Server
    # https://www.microsoft.com/en-us/download/details.aspx?id=56567

#HOW TO USE

    #Create a bat file on a Windows computer that has the contents below
    #**************************
    #start sql_to_bq.exe "Path to SQL settings folder"
    #**************************

    #The SQL settings folder will have 2 files

    #bq_info.txt

    #query_file
        #The first portion of the file name before the "-" will be used as a DB shortname
        #The server mappings are in the db_map.json file in this format { "db_shortname" : [[server1, db1], [server2, db3]] }

        #NOTES:
        # You dont need to write a BQ schema so it pulls column names from SQL Server and uses those as the
        # table names for BQ and automatically determines the data type for the BQ column based on the data used in your SQL query.
        # BQ Does not like spaces so don't use spaces in your column names or this will error out.

#*********************************************************************************************

#Change Log
#____________________________
# 9/23/2021 - Change log to Post to PubSub
# 12/10/2021 - If a problem is encountered a database it will move on to the next file
# 1/27/2021 - Add some validation checks to specify data type errors

#Get the Source IP Address
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
machine_ip = s.getsockname()[0]
s.close()
start_time = datetime.datetime.now()

def pub_message(destination='', path='', message=''):
    time_delta = (datetime.datetime.now()-start_time)
    insert_row = {
        'Program' : 'SQL to BQ',
        'Destination' : destination,
        'Machine_IP' : machine_ip,
        'Path' : path,
        'Message' : message,
        'Timestamp' : datetime.datetime.now(),
        'Duration' : time_delta.total_seconds()
    }
    service_account_info = json.load(open("gcp-sa.json"))
    audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
    creds = jwt.Credentials.from_service_account_info(
        service_account_info, audience=audience
    )
    data = str(json.dumps(insert_row, sort_keys=True, indent=4, default=str)).encode()
    publisher = pubsub_v1.PublisherClient(credentials=creds)
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id='your-gcp-project',
        topic='your-pubsub-topic',
    )
    entry = publisher.publish(topic_name, data)
    entry.result()



def resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)

current_dir = os.getcwd()
bq_creds = service_account.Credentials.from_service_account_file(resource_path('bq_creds.json'))

def getServerDbName(shortString):
    lowerString = str(shortString).lower()
    serverDBs = []
    try:
        with open('db_map.json') as content:
            db_map = json.loads(content.read())
            for setting in db_map:
                for server in setting:
                    if lowerString == server:
                        for db_setting in setting[server]:
                            db_server = db_setting[0]
                            db_name = db_setting[1]
                            serverDBs.append([db_server, db_name])
                        continue
        print(serverDBs)
    except Exception as e:
        print('Error Processing db_map.json file :' + e)
    return serverDBs

#Connects to SQL Server, runs the query and returns a dataframe
def SQLConnect(serverDBs, query):
    try:
        print('Connecting to SQL Server...')
        # for serverDB in serverDBs:
        connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s;DATABASE=%s;TRUSTED_CONNECTION=yes;Integrated Security=SSPI' % (serverDBs[0], serverDBs[1])

        openConnection = pyodbc.connect(connectionString)
        print('Connection Successful')
        print('Running Query...')
        df = pd.read_sql(query, openConnection)
        print('Reading Results into a DataFrame...')
        return df
    except Exception as e:
        print('Connection to SQL Server Failed', str(e))
        return str(e)

def CheckBQSchema(df, bq_project_id, bq_dest_table, bq_table_schema):
    bq_dataset = bq_dest_table.split('.')[0]
    bq_to_table = bq_dest_table.split('.')[1]
    schema_query = """
        SELECT table_catalog, table_schema, table_name, ddl
        FROM """ + bq_project_id + "." + bq_dataset + """.INFORMATION_SCHEMA.TABLES
        WHERE table_name = '""" + bq_to_table + """'
    """

    results_df = pd.read_gbq(schema_query, project_id=bq_project_id, credentials=bq_creds)
    ddl = results_df.loc[results_df.index[0], 'ddl'].strip('\);')
    ddl_start = re.search('\(', ddl).end()
    ddl = ddl[ddl_start:]
    matches = re.findall(r'\w+\s+\w+', ddl)
    table_schema = {}
    for match in matches:
        col =  match.strip(',').split()
        col_name =col[0]
        col_datatype = col[1].strip('64')
        table_schema[col_name] = col_datatype

    results_schema = {}
    if bq_table_schema != '':
        for col in ast.literal_eval(bq_table_schema):
            col_name = col['name']
            col_datatype = col['type']
            results_schema[col_name] = col_datatype
    else:
        for c in df:
            col_name = c
            col_datatype = str(df[c].dtype)
            if col_datatype == 'object':
                col_datatype = 'STRING'
            elif 'float' in col_datatype:
                col_datatype = 'FLOAT'
            elif col_datatype == 'boolean':
                col_datatype = 'BOOLEAN'
            elif 'datetime' in col_datatype:
                col_datatype = 'TIMESTAMP'
            elif 'integer' in col_datatype:
                col_datatype = 'INTEGER'

            results_schema[col_name] = col_datatype
    if len(table_schema) != len(results_schema):
        return 'The number of columns in the query results do not match the number of columns in the BQ table'
    else:
        possible_issues = []
        for c in table_schema:
            try:
                lookup = results_schema[c]
                if lookup != table_schema[c]:
                    possible_issues.append(c + ' column is a ' + lookup + ' in the query results and a ' + table_schema[c] + ' in BigQuery')
            except Exception as err:
                possible_issues.append('Column ' + c + ' does not exist in query results')
        return ','.join(possible_issues)


#Takes a dataframe, BQ settings and pushes the dataframe to Big Query
def BQPush(df, bq_project_id, bq_dest_table, bq_if_exists, bq_table_schema):
    try:
        if bq_table_schema != '':
            print('Setting BQ table schema...')
            bq_tab_schema = ast.literal_eval(bq_table_schema)
            print('Exporting Data to Big Query...')
            gbq.to_gbq(df, destination_table=bq_dest_table, project_id=bq_project_id, credentials=bq_creds,if_exists=bq_if_exists, table_schema=bq_tab_schema,chunksize=10000, progress_bar=True)
        else:
            print('Exporting Data to Big Query...')
            gbq.to_gbq(df, destination_table=bq_dest_table, project_id=bq_project_id, credentials=bq_creds, if_exists=bq_if_exists, chunksize=10000, progress_bar=True)
        return 'Success'
    except Exception as e:
        if str(e) == 'Please verify that the structure and data types in the DataFrame match the schema of the destination table.':
            error_message = CheckBQSchema(df, bq_project_id, bq_dest_table, bq_table_schema)
            print('Error While Exporting to BigQuery...', error_message)
            return 'Error While Exporting to BigQuery: ' + error_message
        elif 'malformed node or string on' in str(e):
            error_message = 'Error While Exporting to BigQuery: Your bq_table_schema.txt file may be incorrect'
            print(error_message)
            return error_message
        else:
            print('Error While Exporting to BigQuery...', str(e))
            return 'Error While Exporting to BigQuery: ' + str(e)

#This is the main function when program is run, it takes a directory path to your BQ settings file and query file(s)
def main():
        print('Program Starting***')
        sys_args = sys.argv
        final_df = pd.DataFrame()
        bq_project_id = ''
        bq_dest_table = ''
        bq_if_exists = ''
        bq_table_schema = ''
        settings_folder_path = sys_args[1]
        settings_dir = os.listdir(settings_folder_path)

        #Loops through each file in the given directory
        for file in settings_dir:
            full_file = settings_folder_path + "\\" + file
            #Takes the Big Query settings file and pulls the BQ project, table, and if exists setting
            # try:
            if 'bq_info' in file:
                bq_config_file = open(full_file, 'r')
                bq_settings = bq_config_file.read().split()
                bq_project_id = bq_settings[0]
                bq_dest_table = bq_settings[1]
                bq_if_exists = bq_settings[2]
                bq_config_file.close()
            #Every other file is treated as a query file
            #Takes the name of the file and splits it by "-", the first section is a short name for the database used in the getServerDBName function
            elif 'bq_table_schema' in file:
                with open(full_file, 'r') as bq_schema_file:
                    bq_table_schema = bq_schema_file.read()
            else:
                try:
                    sql_settings = file.split('-')
                    sql_db = sql_settings[0]
                    serverDBs = getServerDbName(sql_db)
                    sql_query_name = sql_settings[1]
                    query_file = open(full_file, 'r')
                    #Reads the query from the contents of the file
                    query = query_file.read()
                    query_file.close()

                    #Runs the query
                    for server in serverDBs:

                        results = SQLConnect(server, query)
                        if isinstance(results, pd.DataFrame):
                        #Adds the dataframe to the final dataframe
                            final_df = pd.concat([final_df, results])
                        else:
                            print("Error processing " + file + " " + results)
                            pub_message(bq_dest_table, settings_folder_path,"Error processing " + file + " Server: " + server + " " + results)

                # If errors occurs it ignores the file and moves to the next one
                except Exception as e:
                    print("Error processing " + file, e)
                    pub_message(bq_dest_table, settings_folder_path, "Error processing " + file + " " + str(e))
                    continue

        # Once it has the final dataframe, it checks to make sure the BQ settings have been set, then pushes the final dataframe to Big Query
        if bq_project_id != '':
            result = BQPush(final_df, bq_project_id, bq_dest_table, bq_if_exists, bq_table_schema)
            df_len = len(final_df)
            if result == 'Success':
                pub_message(bq_dest_table, settings_folder_path, 'Success: Pushed ' + str(df_len) + ' rows' )
            else:
                pub_message(bq_dest_table, settings_folder_path, result)
        print('***Program Finished***')

if __name__ == "__main__":
    main()