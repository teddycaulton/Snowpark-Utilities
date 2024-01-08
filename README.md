# Snowpark-Utilities
## Description
Snowpark Utilities is a set of Python tools aimed at easing much of the repetitive work around using Snowflake's Snowpark API.  these tools aim make it easier to stand up new snowpark sessions or execute sql commands especialy in environments where multiple sessions are needed.  The module contains functionality for users who want to directly feed credentails for authentication or those who are working with a tool like AWS secrets where credentils might be stored.  The aim of this project is to make it faster and cleaner to stand up new snowpark projects without copy and pasting code from similar endeavors or combing through documentation.

## __init__
if utilizing aws secrets to provider snowflake credentials, you must specify cloud_provider = "aws" or cloud_provider = "AWS" as well as the proper region name, access key id and secret access key

## fetch_credentials_from_secrets(secret_name)
This function takes an AWS secret name as an input and returns the credentials in a dictionary format where they can be queried either for use in create_snowpark_session() or for other uses

## create_snowpark_session(username, password, account, role, warehouse)
This function takes in the above five required inputs and returns a "session" variable which can be used for snowpark operations.  if you were to, for example, have a "parent" and "child" snowflake account and needed sessions for both, you could run the following:
```
parent_session = create_snowpark_session('username', 'password', 'account', 'role', 'warehouse')
child_session = create_snowpark_session('username', 'password', 'account', 'role', 'warehouse')
```
now it's simple to differentiate execution between the two accounts

## aws_create_snowpark_session(secret_name, role, warehouse)
This function is a version of create_snowpark_session() made explicitely for use with AWS Secrets.  simply feed it the appropriate secret name and as long as the username is filed under the key name "username" password under "password" and account under "account" it will return a session.  If you don't have this naming schema and still want to use secrets, it's simple to modify this function or fetch the credentials using fetch_credentials_from_secrets and parse the dictionary yourself

## execute_sql(session, command)
An annoyance I've had with snowpark in terms of ease of use and code readability is that defining code and executing code are two distinctly different operations.  you can always define a piece of SQL code for operation using session.sql("sql code") but executing requires a .collect() at the end of this line.  This command takes in the given session and desired command and executes it all at once.  if you wish to do anything with .to_pandas() you will still need to define that manualy but this works great for anything else and the function returns the .collect() so you could also run the function within a pd.Dataframe()
