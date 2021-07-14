# Migration of Oracle Database to Microsoft SQL Server using SSMA and cx-Oracle (Python), and it’s optimization.
# 
# 
# This script will fast migrate a data-table from the specified OracleDB to Microsoft SQL Server, by converting 
# the database into a Pyspark dataframe followed by schema-inference, and push the dataframe to the SQL Server 
# using 'jdbc'. 
# Note: Replace all credentials and client-paths with your own paths, and you're good to go.
# Results: Time required to migrate a table (with ~3M records) of size 2.25GB : 05:09
# 
# Author - Apurv Purohit
# Date Created - 29/06/2021
import time
import findspark
findspark.init()
from pyspark.sql.types import *
import numpy as np
from pyspark.sql import session
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, lit
from pyspark.sql import SQLContext
from pyspark.sql.functions import to_date
from pyspark.sql.functions import date_format
from pyspark.sql.functions import col,udf
import pyodbc
import os
import urllib
import sqlalchemy
import pandas as pd
import Config as config
from datetime import datetime
import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine
spark = SparkSession.builder.appName("ETL")\
        .config("spark.driver.extraClassPath","sqljdbc42.jar")\
        .getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
def write_into_database(dataframe, database, table_name,  mode):
    dataframe.write \
        .format("jdbc") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("url","jdbc:sqlserver://wn000022700:1433;databaseName="+database) \
        .option("dbtable", table_name) \
        .option("user", config.Dev['user']) \
        .option("password", config.Dev['passwd']) \
        .mode(saveMode=mode) \
        .save()
sqlContext = SQLContext(spark)
# Set Oralce Connection
cx_Oracle.init_oracle_client(
    lib_dir=r"C:\Users\apurohi9\Downloads\instantclient-basic-windows.x64-19.11.0.0.0dbru\instantclient_19_11")
dsn_tns = cx_Oracle.makedsn(config.Oracle['db_server'], config.Oracle['port'], service_name=config.Oracle['service_name'])
conn = cx_Oracle.connect(user=config.Oracle['user'], password=config.Oracle['passwd'], dsn=dsn_tns)
# Open cursor
cursor = conn.cursor()
print(conn)
print(conn.version)
#buidling sql statement to select records from Oracle
sql = "SELECT * FROM POWERFACETS.AUDIT_CMC_MEME_MEMBER"
data=pd.read_sql(sql,conn,chunksize=300000)
res=0
tempfile = 'temp.csv'
if(os.path.exists(tempfile) and os.path.isfile(tempfile)):
    os.remove(tempfile)
for chunk in data:
    chunk.to_csv(tempfile, mode='a')
    res += len(chunk.index)
    print("Chunk to CSV(Records written):",res, end='\r')
print('Temp Csv created!')
df = spark.read\
     .format("csv")\
     .option("header", "true")\
     .option("inferSchema", "true")\
     .load(tempfile)
TableName = 'AUDIT_CMC_MEME_MEMBER'
DatabaseName = "POWERFACETS"
df = df.drop('_c0')
print("Insertion Begins:")
start = time.time()
write_into_database(df, DatabaseName, TableName, 'Append')
print("Rows Inserted:", df.count())
end = time.time()
hours, rem = divmod(end - start, 3600)
minutes, seconds = divmod(rem, 60)
os.remove(tempfile)
print("Data successfully pushed to SQL Server!")
print("\nTime taken: {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
# close connection
# conn.close()
