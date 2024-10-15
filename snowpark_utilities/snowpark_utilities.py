import boto3
import json
from snowflake.snowpark.session import Session
import pandas as pd
import logging

class snowpark_utilities:
    def __init__(self, region_name = 'us-east-1', cloud_provider = "aws", aws_access_key_id = "", aws_secret_access_key = ""):
        # define normal variables
        if cloud_provider == "aws" or cloud_provider == "AWS":
            self.region_name = region_name
            self.aws_access_key_id = aws_access_key_id
            self.aws_secret_access_key = aws_secret_access_key
    
    def fetch_credentials_from_secrets(self, secret_name):
        try:
            session = boto3.Session( 
            aws_access_key_id = self.aws_access_key_id, 
            aws_secret_access_key = self.aws_secret_access_key 
            )
            client = session.client(
                service_name='secretsmanager',
                region_name=self.region_name
            )
            response = client.get_secret_value(
                SecretId = secret_name 
            )     
            credentials = json.loads(response['SecretString'])
            return credentials
        except:
            logging.error("An error occurred on service side")
        return 0

    @staticmethod
    def create_snowpark_session(username, password, account, role = "ACCOUNTADMIN", warehouse = "COMPUTE_WH"):

        connection_params = {
        "user" : username,
        "password" : password,
        "account" : account,
        "role" : role,
        "warehouse" : warehouse
        }
        # create snowpark session
        session = Session.builder.configs(connection_params).create()
        return session
    
    def aws_create_snowpark_session(self, secret_name, role = "ACCOUNTADMIN", warehouse = "COMPUTE_WH"):
        credentials = self.fetch_credentials_from_secrets(secret_name=secret_name)
        username = credentials["username"]
        password = credentials["password"]
        account = credentials["account"]
        session = self.create_snowpark_session(username = username, password = password, account = account, role = role, warehouse = warehouse)
        return session

    @staticmethod
    def execute_sql(session, command):
        sql_command = session.sql(command)
        return sql_command.collect()
        
    @staticmethod
    def execute_sql_pandas(session, command):
        sql_command = session.sql(command)
        return sql_command.to_pandas()
    
    def snowflake2snowflakevalidation(self, session_source, session_target, database):
        results_db = pd.DataFrame()
        databases = []
        schemas = []
        tables = []
        source_count = []
        target_count = []
        source_db = self.execute_sql(session_source, f"SELECT DATABASE_NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME='{database}'")
        target_db = self.execute_sql(session_target, f"SELECT DATABASE_NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME='{database}'")
        if source_db!=target_db:
            logging.error(f"Database {database} could not be found in either the source or target account.  please check spelling and the existence of the database in both accounts")

        schemas_source = pd.DataFrame(self.execute_sql(session_source, f"SHOW SCHEMAS IN DATABASE {database}"))
        for schema in schemas_source["name"]:
            schema_target = pd.DataFrame(self.execute_sql(session_target, f"SHOW SCHEMAS LIKE '{schema}'"))
            tables_source = pd.DataFrame(self.execute_sql(session_source, f"SHOW TABLES IN SCHEMA {database}.{schema}"))
            if tables_source.empty:
                continue
            
            if schema_target["name"][0] == schema and schema_target["database_name"][0] == database:
                for table in tables_source["name"]:
                    try:
                        if table.isupper():
                            source_row_count = self.execute_sql_pandas(session_source, f"SELECT COUNT(*) FROM {database}.{schema}.{table}")
                            source_row_count = source_row_count["COUNT(*)"][0]
                            target_row_count = self.execute_sql_pandas(session_target, f"SELECT COUNT(*) FROM {database}.{schema}.{table}")
                            target_row_count = target_row_count["COUNT(*)"][0]
                        else:
                            source_row_count = self.execute_sql_pandas(session_source, f'SELECT COUNT(*) FROM {database}.{schema}."{table}"')
                            source_row_count = source_row_count["COUNT(*)"][0]
                            target_row_count = self.execute_sql_pandas(session_target, f'SELECT COUNT(*) FROM {database}.{schema}."{table}"')
                            target_row_count = target_row_count["COUNT(*)"][0]
                        if source_row_count == target_row_count:
                            databases.append(database)
                            schemas.append(schema)
                            tables.append(table)
                            source_count.append(source_row_count)
                            target_count.append(target_row_count)
                        elif source_row_count != target_row_count:
                            databases.append(database)
                            schemas.append(schema)
                            tables.append(table)
                            source_count.append(source_row_count)
                            target_count.append(target_row_count)
                    except Exception as e:
                        source_row_count = "NA"
                        target_row_count = "NA"
                        databases.append(database)
                        schemas.append(schema)
                        tables.append(table)
                        source_count.append(source_row_count)
                        target_count.append(target_row_count)
            else:
                print(f"Schema '{schema}' does not exist in the target table")
        
        results_db["database"] = databases
        results_db["schema"] = schemas
        results_db["table"] = tables
        results_db["source_count"] = source_count
        results_db["target_count"] = target_count

        return results_db
    
    @staticmethod
    def parse_csv(df, varchar):

        table_info = pd.DataFrame(columns = ['Column Name', 'Data Type'])

        for column in df.columns.values.tolist():
            if varchar:
                table_info.loc[len(table_info.index)] = [column, 'varchar(16777216)']
            elif (df[column].dtype.name == "int" or df[column].dtype.name == "int64"):
                table_info.loc[len(table_info.index)] = [column, 'int']
            elif df[column].dtype.name == "object":
                table_info.loc[len(table_info.index)] = [column, 'varchar(16777216)']
            elif df[column].dtype.name == "datetime64[ns]":
                table_info.loc[len(table_info.index)] = [column, 'datetime']
            elif df[column].dtype.name == "float64":
                table_info.loc[len(table_info.index)] = [column, 'float8']
            elif df[column].dtype.name == "bool":
                table_info.loc[len(table_info.index)] = [column, 'boolean']
            else:
                table_info.loc[len(table_info.index)] = [column, 'varchar(16777216)']
        
        return table_info

    def create_table_statement(self, database,schema,table, df, uppercase = False, varchar = False):
        table_info = self.parse_csv(df, varchar)
        ## Create the table if it doesn't exist:
        create_tbl_statement = f'CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} ('

        table_info = table_info.reset_index()
        for index, row in table_info.iterrows():
            if uppercase:
                create_tbl_statement = create_tbl_statement + '"' + row['Column Name'].upper() + '" ' + row['Data Type'] + ', '
            else:
                create_tbl_statement = create_tbl_statement + '"' + row['Column Name'] + '" ' + row['Data Type'] + ', '
        create_tbl_statement = create_tbl_statement[:-2] + ');'
            
        return create_tbl_statement
