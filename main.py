import configparser
from snowflake.snowpark import Session
import os

from snowflake.snowpark.functions import udf

# connection_parameters = {
#     "account": os.environ["snowflake_account"],
#     "user": os.environ["snowflake_user"],
#     "password": os.environ["snowflake_password"],
#     "role": os.environ["snowflake_user_role"],
#     "warehouse": os.environ["snowflake_warehouse"],
#     "database": os.environ["snowflake_database"],
#     "schema": os.environ["snowflake_schema"]
# }

config = configparser.ConfigParser()
ini_path = os.path.join(os.getcwd(),'config.ini')
config.read(ini_path)

connection_parameters = {
    "account": config['Snowflake']['SF_ACCOUNT'],
    "user": config['Snowflake']['SF_USER'],
    "password": config['Snowflake']['SF_PASS'],
    "warehouse": config['Snowflake']['SF_WAREHOUSE'],
    "database": config['Snowflake']['SF_DATABASE'],
    "schema": config['Snowflake']['SF_SCHEMA']
}


if __name__ == '__main__':
    print("hello world")
    print(connection_parameters)
    session = Session.builder.configs(connection_parameters).create()

    # @udf(name="add", is_permanent=False)
    # def udf(x: int) -> int:
    #     return x + 1
    print("Computing....")
    dfUsers = session.sql("SELECT count(*) FROM users")
    dfUsers.show()
    print(dfUsers.collect())
    print("===> END")



