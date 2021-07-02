import time
import findspark
findspark.init()
from pyspark.sql.types import *
import numpy as np
from pyspark.sql import session
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, lit
import pyodbc
import urllib
import sqlalchemy
import pandas as pd
import Config as config
from datetime import datetime
loaddate = str(datetime.now())[:-4]
spark = SparkSession.builder.appName("ETL")\
        .config("spark.driver.extraClassPath","sqljdbc42.jar")\
        .getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

def execute_sql(db, query,record_list):
    try:
        connection = pyodbc.connect('driver={%s};server=%s;database=%s;uid=%s;pwd=%s' % (config.Dev['driver'],
                                                                                         config.Dev['db_server'], db,
                                                                                         config.Dev['user'],
                                                                                         config.Dev['passwd']))
        cursor = connection.cursor()
        print(query)
        if "insert" in query:
            if not record_list :
                cursor.execute(query)
                print("Executed the query")
            else :
                cursor.executemany(query,record_list)
                print("Executed the query")
            connection.commit()
        else:
            cursor.execute(query)
            print("Executed the query")
            return cursor
    except Error as err:
        print(f"Error: '{err}'")

def write_into_database(dataframe, database, table_name,  mode):
    dataframe.write \
        .format("jdbc") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("url","jdbc:sqlserver://Dbsed4555:1433;databaseName=Eligibility") \
        .option("dbtable", table_name) \
        .option("user", config.Dev['user']) \
        .option("password", config.Dev['passwd']) \
        .mode(saveMode=mode) \
        .save()

def writefile():
    empty_list = []
    write_query = """ 
                      select [ProductType],[Role],[LastName],[FirstName],[MiddleInitial],[Gender],[DOB],[Specialty1],[Specialty2],[Specialty3],[Specialty4],[Address1],[Address2],[City],[State],[ZipCode],
                      [PhoneNumber],[FaxNumber],GLOBAL.dbo.fn_decrypt(SSN)+RIGHT(SSN,4) as SSN,[FedTaxID],[UPIN] from [UhcProvider].[dbo].[tblMyUhcProvider] 
                  """
    cursor = execute_sql("UhcProvider", write_query, empty_list)
    result = cursor.fetchall()
    print(type(result))
    f = open("JOB_364.txt", "a")
    line = ''
    for tup in result:
        line = ''
        for v in tup:
            if(v is None):
                line += ','
            else:
                v = '"'+v+'"'
                line += v
                line += ','
        line = line[:-1]
        line += '\n'
        f.write(line)
    f.close()
    print("File written!")

def main():
    try:
        writefile()
        return 0
    except Error as err:
        return 40

if __name__ == "__main__":
    main()