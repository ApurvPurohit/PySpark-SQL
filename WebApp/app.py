import time
import findspark
findspark.init()
from pyspark.sql.types import *
import numpy as np
from pyspark.sql import session
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, lit, to_date, date_format, col, udf
from pyspark.sql import SQLContext
from flask import Flask, render_template, request, redirect
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
spark = SparkSession.builder.appName("ETL") \
        .config("spark.driver.extraClassPath", "sqljdbc42.jar") \
        .getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
# Set Oralce Connection
cx_Oracle.init_oracle_client(
    lib_dir=r"C:\Users\apurohi9\Downloads\instantclient-basic-windows.x64-19.11.0.0.0dbru\instantclient_19_11")
dsn_tns = cx_Oracle.makedsn('es20-scan01', '1521', service_name='cmc1st01svc.uhc.com')
conn = cx_Oracle.connect(user='POWERFACETS_MIGRATION', password='t;.<4vQj', dsn=dsn_tns)
# Open cursor
cursor = conn.cursor()
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
def multiple_table_migration():
    k = 1
    total = ""
    with open("table_file.txt") as f:
        for line in f:
            tup = line.split("*")
            db = tup[0].strip()
            tbl = tup[1].strip()
            print('Migration details received for Table', tbl)
            print(db,tbl)
            status, tt = migratedb(db, tbl)
            total = "Table " + tbl + "| " + tt + "\n"
            k += 1
    status = "All tables in the table_file have been successfully migrated!"
    return (status,total)

def migratedb(dbname, tblname):
    print("Migration Begins!")
    sql = "SELECT * FROM "+dbname+"."+tblname
    data = pd.read_sql(sql, conn, chunksize=300000)
    res = 0
    file = open('mig-temp.csv','a')
    tempfile = 'mig-temp.csv'
    for chunk in data:
        chunk.to_csv(tempfile, mode='a')
        res += len(chunk.index)
        print("Chunk to CSV(Records written):", res, end='\r')
    print('\n')
    print('Temp Csv created!')
    isempty = os.path.getsize(tempfile) == 0
    if (isempty):
        status1 = "No data present at source!"
        return (status1, "000")
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(tempfile)
    TableName = tblname
    DatabaseName = dbname
    df = df.drop('_c0')
    print("Insertion Begins:")
    start = time.time()
    write_into_database(df, DatabaseName, TableName, 'Append')
    print("Rows Inserted:", df.count())
    end = time.time()
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    file.close()
    os.remove(tempfile)
    status = "Data successfully pushed to SQL Server!"
    print(status)
    print('Temp Csv removed!')
    print("Migration Ends :)")
    tt = "\nTime taken: {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds)
    print(tt)
    return (status, tt)


app = Flask(__name__)

@app.route('/')
def index():
  return render_template('index.html')

@app.route('/migrate/', methods=['POST'])
def migrate():
    database_name = request.form['dbname']
    tablename = request.form['tblname']
    print ('Migration Details Received!')
    if((database_name == '#') and (tablename == '#')):
        status, tt = multiple_table_migration()
    else:
        status, tt = migratedb(database_name,tablename)
    templateData = {
        'status': status,
        'time': tt
    }
    return render_template('result.html', **templateData)

if __name__ == '__main__':
  app.run(debug=True)