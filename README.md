# Snowpark-Utilities
[![Downloads](https://static.pepy.tech/badge/snowpark-utilities/month)](https://pepy.tech/project/snowpark-utilities)
## Description
Snowpark Utilities is a set of Python tools aimed at easing much of the repetitive work around using Snowflake's Snowpark API.  these tools aim make it easier to stand up new snowpark sessions or execute sql commands especialy in environments where multiple sessions are needed.  The module contains functionality for users who want to directly feed credentails for authentication or those who are working with a tool like AWS secrets where credentils might be stored.  The aim of this project is to make it faster and cleaner to stand up new snowpark projects without copy and pasting code from similar endeavors or combing through documentation.

## __init__
if utilizing aws secrets to provider snowflake credentials, you must specify cloud_provider = "aws" or cloud_provider = "AWS" as well as the proper region name, access key id and secret access key

## fetch_credentials_from_secrets(secret_name)
This function takes an AWS secret name as an input and returns the credentials in a dictionary format where they can be queried either for use in create_snowpark_session() or for other uses
```python
from snowpark_utilities import snowpark_utilities  # Replace 'your_module_name' with the actual module name.

# Instantiate the class with custom arguments if needed
snowpark_util = snowpark_utilities(
    region_name="us-west-2",
    cloud_provider="aws",
    aws_access_key_id="your_access_key",
    aws_secret_access_key="your_secret_key"
)

# Call the method with the secret name
credentials = snowpark_util.fetch_credentials_from_secrets("your_secret_name")
```

## create_snowpark_session(username, password, account, role, warehouse)
This function takes in the above five required inputs and returns a "session" variable which can be used for snowpark operations.  if you were to, for example, have a "parent" and "child" snowflake account and needed sessions for both, you could run the following:
```python
from snowpark_utilities import snowpark_utilities as spu
parent_session = spu.create_snowpark_session('username', 'password', 'account', 'role', 'warehouse')
child_session = spu.create_snowpark_session('username', 'password', 'account', 'role', 'warehouse')
```
now it's simple to differentiate execution between the two accounts

## aws_create_snowpark_session(secret_name, role, warehouse)
This function is a version of create_snowpark_session() made explicitely for use with AWS Secrets.  simply feed it the appropriate secret name and as long as the username is filed under the key name "username" password under "password" and account under "account" it will return a session.  If you don't have this naming schema and still want to use secrets, it's simple to modify this function or fetch the credentials using fetch_credentials_from_secrets and parse the dictionary yourself.  Here's an easy example of how you might use this
```python
from snowpark_utilities import snowpark_utilities  # Replace 'your_module_name' with the actual module name.

# Instantiate the class with custom arguments if needed
snowpark_util = snowpark_utilities(
    region_name="us-west-2",
    cloud_provider="aws",
    aws_access_key_id="your_access_key",
    aws_secret_access_key="your_secret_key"
)

# Call the method with the secret name
credentials = snowpark_util.aws_create_snowpark_session("your_secret_name")
```

## execute_sql(session, command)
An annoyance I've had with snowpark in terms of ease of use and code readability is that defining code and executing code are two distinctly different operations.  you can always define a piece of SQL code for operation using session.sql("sql code") but executing requires a .collect() at the end of this line.  This command takes in the given session and desired command and executes it all at once.  if you wish to do anything with .to_pandas() you will still need to define that manualy but this works great for anything else and the function returns the .collect() so you could also run the function within a pd.Dataframe()
```python
spu.execute_sql("USE ROLE DATA_ANALYST")
```

## execute_sql_pandas(session, command)
created command that given a SELECT command returns a pandas dataframe
```python
dataframe = spu.execute_sql_pandas("SELECT * FROM DWLOAD.DWSTAGE.TABLE1")
```

## snowflake2snowflakevalidation(session_source, session_target, database)
In the case of database migration, it can be time consuming to ensure all tables were successfully migrated. this function takes in a source and target snowpark session along with the database in question and returns a dataframe of all tables from the source and the associated row count between the two tables.
```python
parent_session = spu.create_snowpark_session('username', 'password', 'account', 'role', 'warehouse')
child_session = spu.create_snowpark_session('username', 'password', 'account', 'role', 'warehouse')
spu.snowflake2snowflakevalidation(parent_session, child_session, "DWLOAD")
```

## create_table_statement(database, schema, table, df, uppercase = False, varchar = False)
In the case of database migration, you usually need to stand up all target tables prior to staging data.  On large engagements where there can be upwards of 1000 tables to migrate having an automated method to generate these create table statements is crucial. This function takes inputs around the table info alongside a dataframe of that table and generates a create table statment.  there are optional parameters for forcing uppercase column names or setting all data types to VARCHAR for raw layers.  just loop through a list of 1 row table examples from a legacy source and concatenate the outputs into one .sql file for execution.
```python
df = pd.read_csv('SampleFile2019.csv')
foo = create_table_statement(database = 'foo', schema = 'bar', table = 'man', df)
print(foo)
```