import configparser
import os

import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import call_udf
from snowflake.snowpark.functions import udf, col, lit, udtf
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType

print("===> reading config file")
config = configparser.ConfigParser()
ini_path = os.path.join(os.getcwd(), 'config.ini')
config.read(ini_path)


def getConfig():
    return config['Snowflake']


connection_parameters = {
    "account": getConfig()['SF_ACCOUNT'],
    "user": getConfig()['SF_USER'],
    "password": getConfig()['SF_PASS'],
    "warehouse": getConfig()['SF_WAREHOUSE'],
    "database": getConfig()['SF_DATABASE'],
    "schema": getConfig()['SF_SCHEMA']
}


def readDataFromSQL():
    print("===> Showing other data base.")
    usersSQL = session.sql("SELECT * FROM users")
    usersSQL.show()
    print(usersSQL.collect())


def readAndWriteInNewTable():
    dfUsers = session.table("users")
    dfUsers.show()
    print("===> Transforming.")
    dfTransformed = dfUsers.groupBy(col("age")).count()
    dfTransformed.show()
    print("===> Writing.")
    dfTransformed.write.mode("overwrite").save_as_table("usersTransformed")


def readFromS3toSf():
    dfStaging = session.read \
        .schema(
        StructType([
            StructField("plant_name", StringType()),
            StructField("UOM", StringType()),
            StructField("Low_End_of_Range", StringType()),
            StructField("High_End_of_Range", StringType())
        ])
    ).option("field_delimiter", ",") \
        .option("SKIP_HEADER", 1) \
        .option("FIELD_OPTIONALLY_ENCLOSED_BY", '"') \
        .csv("@stage/veg_plant_height.csv")
    dfStaging.show()
    dfStaging.write.mode("overwrite").save_as_table("dataFromS3")


def readLocalCSVtoSF():
    df = pd.read_csv('data/users.csv')
    print(df)
    print("===> write pandas")
    df.columns = map(lambda x: str(x).upper(), df.columns)
    session.write_pandas(df=df,
                         table_name="USERS_FROM_LOCAL_CSV",
                         quote_identifiers=False)


def addUDFandApply():
    @udf(name="add", is_permanent=True, stage_location="@stage", replace=True)
    def add(x: int) -> int:
        return x + 1

    dfUsers = session.table("users")
    dfUsers.select(col("name"), add(col("age"))) \
        .show()


def applyRegisteredUDF():
    dfUsers = session.table("users")
    dfUsers.select(col("name"),
                   call_udf("add", col("age"))).show()


def runUDTFexample():
    class GeneratorUDTF:
        def process(self, n):
            for i in range(n):
                yield (i,)

    generator_udtf = udtf(GeneratorUDTF,
                          output_schema=StructType([StructField("number", IntegerType())]),
                          input_types=[IntegerType()])
    rows = session.table_function(generator_udtf(lit(3)))
    rows.show()
    print(rows.collect())

def registerUDTF():
    from collections import Counter
    from typing import Iterable, Tuple
    class MyWordCount:
        def __init__(self):
            self._total_per_partition = 0

        def process(self, s1: str) -> Iterable[Tuple[str, int]]:
            words = s1.split()
            self._total_per_partition = len(words)
            counter = Counter(words)
            yield from counter.items()

        def end_partition(self):
            yield ("partition_total", self._total_per_partition)
    udtf_name = "word_count_udtf"
    word_count_udtf = session.udtf.register(
        MyWordCount,
        ["word", "count"],
        name=udtf_name,
        is_permanent=True,
        replace=True,
        stage_location="@stage"
    )


def applyUDTF():
    udtf_name = "word_count_udtf"
    df1 = session.table_function(udtf_name, lit("w1 w2 w2 w3 w3 w3"))
    df1.show()

def registerStoreProcedure():
    import snowflake.snowpark
    from snowflake.snowpark.functions import sproc

    @sproc(name="minus_one", is_permanent=True, stage_location="@stage", replace=True,
           packages=["snowflake-snowpark-python"])
    def minus_one(session: snowflake.snowpark.Session, x: int) -> int:
        return session.sql(f"{x} - 1").collect()[0][0]

    # session.call("minus_one", 1)

if __name__ == '__main__':
    # print(connection_parameters)
    session = Session.builder.configs(connection_parameters).create()
    sql = "use warehouse "+getConfig()['SF_WAREHOUSE']
    session.sql(sql)
    print("===> SQL: "+sql)

    print("===> START")

    # readDataFromSQL()
    readAndWriteInNewTable()
    # readFromS3toSf()
    # readLocalCSVtoSF()
    # addUDFandApply()
    # applyRegisteredUDF()

    # registerUDTF()
    # applyUDTF()


    print("===> END")
