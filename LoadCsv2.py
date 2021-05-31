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
        .config("spark.driver.extraClassPath","spark-mssql-connector_2.12_3.0-1.0.0-alpha")\
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

def get_loadnum():
    empty_list=[]
    # insert_Load = "insert into eligibility..t_eligloads values ('"+loaddate+"','MEMBERS.TXT'," \
    #               "'tblHealthNewEnglandLoad','602, 604, 605')"
    # execute_sql("Eligibility",  insert_Load,empty_list)
    selectloadnum = "select LoadNum from eligibility..t_eligloads where FileName = 'MEMBERS.TXT' and LoadDate = '" \
                    + loaddate+"'"
    cursor = execute_sql("Eligibility", selectloadnum,empty_list)
    return str(cursor.fetchone()[0])

def readfile():
    df = spark.read.format("Text").option("header", "true").load("Job213MB_0672_BE_01_ACNWAZ_4670_F_20210521.DAT")
    df2 = df.select(
        lit(get_loadnum()).alias('LoadNum'),
        df.value.substr(1,1).alias('Column 1'),
        df.value.substr(2,3).alias('Column 2'),
        df.value.substr(5,5).alias('Column 3'),
        df.value.substr(10,9).alias('Column 4'),
        df.value.substr(19,2).alias('Column 5'),
        df.value.substr(21,12).alias('Column 6'),
        df.value.substr(33,1).alias('Column 7'),
        df.value.substr(34,18).alias('Column 8'),
        df.value.substr(52,30).alias('Column 9'),
        df.value.substr(82,30).alias('Column 10'),
        df.value.substr(112,15).alias('Column 11'),
        df.value.substr(127,2).alias('Column 12'),
        df.value.substr(129,5).alias('Column 13'),
        df.value.substr(134,4).alias('Column 14'),
        df.value.substr(138,25).alias('Column 15'),
        df.value.substr(163,0).alias('Column 16'),
        df.value.substr(163,1).alias('Column 17'),
        df.value.substr(164,44).alias('Column 18'),
        df.value.substr(208,10).alias('Column 19'),
        df.value.substr(218,10).alias('Column 20'),
        df.value.substr(228,21).alias('Column 21'),
        df.value.substr(249,15).alias('Column 22'),
        df.value.substr(264,20).alias('Column 23'),
        df.value.substr(284,1).alias('Column 24'),
        df.value.substr(285,44).alias('Column 25'),
        df.value.substr(329,3).alias('Column 26'),
        df.value.substr(332,67).alias('Column 27'),
        df.value.substr(399,3).alias('Column 28'),
        df.value.substr(402,4).alias('Column 29')
    )  
    print(type(df2))
    TableName = "PCSecureHorizonsAZ4670MemberLoad"
    DatabaseName = "Eligibility"
    no_of_rows = df2.count()
    # Splitting the Dataframe if size is LARGE
    if(no_of_rows>=300000):
        samples = np.arange(10, 15, 0.5).tolist()
        dfArray = df2.randomSplit(samples, 24)
    else:
        dfArray = []
        dfArray.append(df2)
    params = 'DRIVER=' + config.Dev['driver'] + ';SERVER=' + config.Dev['db_server']  + ';PORT=1433;DATABASE=' + DatabaseName + ';UID=' + config.Dev['user'] + ';PWD=' + config.Dev['passwd']
    db_params = urllib.parse.quote_plus(params)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(db_params))
    # df is the dataframe; test is table name in which this dataframe is #inserted    
    start = time.time()
    for dataf in dfArray:        
        dataf = dataf.toPandas()
        dataf.to_sql(TableName, engine, index=False, if_exists="append", schema="dbo")        
        print("Inserted",dataf.shape[0],"rows.")
    end = time.time()
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    print("\nTime taken: {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))

def main():
    try:
        readfile()
        return 0
    except Error as err:
        return 40

if __name__ == "__main__":
    main()
