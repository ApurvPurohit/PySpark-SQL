# Required:
# Sybase Adaptive Server Enterprise SDK (64-bit) 16.0 - ODBC Driver for Sybase
import pyodbc
import Config as config
import datetime
import logging

serv = config.Sybase['db_server']
usr = config.Sybase['user']
pwd = config.Sybase['passwd']
prt = config.Sybase['port']
driver= config.Sybase['driver']
# Create and configure logger
logging.basicConfig(filename="sybase.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
# Creating an object
logger = logging.getLogger()
# Setting the threshold of logger to INFO
logger.setLevel(logging.INFO)
print (datetime.datetime.now())
try:
    conn = pyodbc.connect(driver=driver, server=serv, port = prt,uid=usr, pwd=pwd)
    print("Connection Established!")
    logger.info("Connection Established!")
    print(conn)
    conn.close()
except Exception as e:
    print('Exception Occurred:', e.__class__.__name__)
    logger.info('Exception Occurred:', e.__class__.__name__)

