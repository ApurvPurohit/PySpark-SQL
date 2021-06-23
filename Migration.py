#Import libraries
import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine
#Set Oralce Connection
dsn_tns = cx_Oracle.makedsn('Host IP or Host name', 'Port Number', service_name='name of your service')
conn = cx_Oracle.connect(user='DB User', password='DB Password', dsn=dsn_tns)
#Open cursor
cursor = conn.cursor()
#buidling sql statement to select records from Oracle
sql = "SELECT * FROM ORACLE_BLOB_TABLE"
#read data into dataframe directly
data=pd.read_sql(sql,conn)
print("Total records form Oracle : ", data.shape[0])
#Create sqlalchemy engine
engine = create_engine("mysql+pymysql://mysql_user:mysql_password@host_name:PORT/Schema_Name")
data.to_sql("Table name", con = engine, if_exists = 'append', index = False, chunksize =10000)
print("Data pushed success")
#close connection
conn.close()
