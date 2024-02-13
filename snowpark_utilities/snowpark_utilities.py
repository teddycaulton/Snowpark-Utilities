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
