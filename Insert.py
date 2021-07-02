import time
import findspark
findspark.init()
from pyspark.sql.types import *
import numpy as np
from pyspark.sql import session
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, lit
from pyspark.sql.functions import to_date
from pyspark.sql.functions import date_format
from pyspark.sql.functions import col,udf
import pyodbc
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
#Set Oralce Connection
cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\apurohi9\Downloads\instantclient-basic-windows.x64-19.11.0.0.0dbru\instantclient_19_11")
dsn_tns = cx_Oracle.makedsn('es20-scan01', '1521', service_name='cmc1st01svc.uhc.com')
conn = cx_Oracle.connect(user='POWERFACETS_MIGRATION', password='t;.<4vQj', dsn=dsn_tns)
#Open cursor
cursor = conn.cursor()
print(conn)
print(conn.version)
#buidling sql statement to select records from Oracle
sql = "SELECT * FROM POWERFACETS.AUDIT_CMC_MEME_MEMBER"
data=pd.read_sql(sql,conn,chunksize=10000)
TableName = 'AUDIT_CMC_MEME_MEMBER'
DatabaseName = "POWERFACETS"
params = 'DRIVER=' + config.Dev['driver'] + ';SERVER=' + config.Dev['db_server']  + ';PORT=1433;DATABASE=' + DatabaseName + ';UID=' + config.Dev['user'] + ';PWD=' + config.Dev['passwd']
db_params = urllib.parse.quote_plus(params)
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(db_params))
start = time.time()
for chunk in data:
    print(dict(chunk.dtypes))
    chunk['MEME_EOI_TERM_DT'] = pd.NaT
    chunk['MEME_PREX_TERM_DT'] = pd.NaT
    chunk['MEME_CCC_END_DT'] = pd.NaT
    chunk['ATXR_SOURCE_ID'] = pd.NaT
    filter1 = chunk.dtypes[chunk.dtypes == np.dtype('datetime64[ns]')]
    # filter2 = chunk.dtypes[chunk.dtypes == np.dtype('<M8[ns]')]
    list1 = list(filter1.index)
    # list2 = list(filter2.index)
    # list1.append(list2)
    print(list1)
    for col in list1:
        chunk[col].apply(lambda x: pd.to_datetime(x, errors='coerce'))
    chunk.to_sql(TableName, engine, index=False, if_exists="append")
    print("Rows Inserted:", len(chunk.index))
end = time.time()
hours, rem = divmod(end - start, 3600)
minutes, seconds = divmod(rem, 60)
print("Data pushed success")
print("\nTime taken: {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
#close connection MEME_CCC_END_DT ATXR_SOURCE_ID
conn.close()