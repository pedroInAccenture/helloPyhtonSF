import configparser
from snowflake.snowpark import Session
from snowflake.snowpark.functions import udf
import os



print("===> reading config file")
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
    print("===> Computing....")
    session.sql("""
        INSERT INTO users (name , age) VALUES ( Pedro, 35);
    """)

    dfUsers = session.sql("SELECT count(*) FROM users")
    dfUsers.show()

    print("===> Showing other data base....")
    plants = session.sql("SELECT * FROM GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS")
    plants.show()
    print(dfUsers.collect())

    print("===> END")



