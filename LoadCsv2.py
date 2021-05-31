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

def executefast_sql(db, TableName, record_list):
    conn = pyodbc.connect('driver={%s};server=%s;database=%s;uid=%s;pwd=%s' % (config.Dev['driver'],
                                                                                     config.Dev['db_server'], db,
                                                                                     config.Dev['user'],
                                                                                     config.Dev['passwd']))

    insert_query="insert into eligibility.dbo.tblHealthNewEnglandLoad values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    cursor = conn.cursor()
    cursor.executemany(insert_query, record_list)
    cursor.commit()
    print(f'{len(record_list)} rows inserted to the table ',TableName)
    cursor.close()
    conn.close()

def get_loadnum():
    empty_list=[]
    insert_Load = "insert into eligibility..t_eligloads values ('"+loaddate+"','MEMBERS.TXT'," \
                  "'tblHealthNewEnglandLoad','602, 604, 605')"
    execute_sql("Eligibility",  insert_Load,empty_list)
    selectloadnum = "select LoadNum from eligibility..t_eligloads where FileName = 'MEMBERS.TXT' and LoadDate = '" \
                    + loaddate+"'"
    cursor = execute_sql("Eligibility", selectloadnum,empty_list)
    return str(cursor.fetchone()[0])

def readfile():
    df = spark.read.format("Text").option("header", "true").load("Job213MB_0672_BE_01_ACNWAZ_4670_F_20210521.DAT")
    df2 = df.select(
        lit(get_loadnum()).alias('LoadNum'),
        df.value.substr(1, 1).alias('RecordType'),
        df.value.substr(2, 10).alias('TPACarrierID'),
        df.value.substr(12, 2).alias('PHSCompanyNum'),
        df.value.substr(14, 7).alias('SubscriberNum'),
        df.value.substr(21, 2).alias('SubscriberSuffix'),
        df.value.substr(23, 20).alias('LastName'),
        df.value.substr(43, 1).alias('MiddleInitial'),
        df.value.substr(44, 15).alias('FirstName'),
        df.value.substr(59, 30).alias('Street'),
        df.value.substr(89, 20).alias('City'),
        df.value.substr(109, 2).alias('State'),
        df.value.substr(111, 9).alias('ZipCode'),
        df.value.substr(120, 10).alias('PhoneNum'),
        df.value.substr(130, 9).alias('SSN'),
        df.value.substr(139, 1).alias('Sex'),
        to_timestamp(df.value.substr(140, 0), 'yyyy-MM-dd HH:mm:ss').alias('DateOfBirth'),
        df.value.substr(140, 3).alias('CoverPlan'),
        df.value.substr(143, 3).alias('BenefitCd'),
        to_timestamp(df.value.substr(146, 0), 'yyyy-MM-dd HH:mm:ss').alias('BenefitStartDate'),
        to_timestamp(df.value.substr(146, 0), 'yyyy-MM-dd HH:mm:ss').alias('BenefitEndDate'),
        df.value.substr(146, 0).cast(DecimalType()).alias('CopayAmt'),
        df.value.substr(146, 1).alias('CopayType'),
        df.value.substr(147, 6).alias('EmpGroup'),
        df.value.substr(153, 6).alias('ProvGroup'),
        df.value.substr(159, 4).alias('ProvFacility'),
        df.value.substr(163, 5).alias('SpecialPlanCd'),
        df.value.substr(168, 1).alias('ErisaInd'),
        df.value.substr(169, 19).alias('Filler'),
        df.value.substr(188, 3).alias('CoverPlan2'),
        df.value.substr(191, 3).alias('BenefitCd2'),
        to_timestamp(df.value.substr(194, 0), 'yyyy-MM-dd HH:mm:ss').alias('BenefitStartDate2'),
        to_timestamp(df.value.substr(194, 0), 'yyyy-MM-dd HH:mm:ss').alias('BenefitEndDate2'),
        df.value.substr(194, 0).cast(DecimalType()).alias('CopayAmt2'),
        df.value.substr(194, 1).alias('CopayType2'),
        df.value.substr(195, 6).alias('EmpGroup2'),
        df.value.substr(201, 6).alias('ProvGroup2'),
        df.value.substr(207, 4).alias('ProvFacility2'),
        df.value.substr(211, 5).alias('SpecialPlanCd2'),
        df.value.substr(216, 1).alias('ERISAInd2'),
        df.value.substr(217, 19).alias('Filler2'),
        df.value.substr(236, 3).alias('CoverPlan3'),
        df.value.substr(239, 3).alias('BenefitCd3'),
        to_timestamp(df.value.substr(242, 0), 'yyyy-MM-dd HH:mm:ss').alias('BenefitStartDate3'),
        to_timestamp(df.value.substr(242, 0), 'yyyy-MM-dd HH:mm:ss').alias('BenefitEndDate3'),
        df.value.substr(242, 0).cast(DecimalType()).alias('CopayAmt3'),
        df.value.substr(242, 1).alias('CopayType3'),
        df.value.substr(243, 6).alias('EmpGroup3'),
        df.value.substr(249, 6).alias('ProvGroup3'),
        df.value.substr(255, 4).alias('ProvFacility3'),
        df.value.substr(259, 5).alias('SpecialPlanCd3'),
        df.value.substr(264, 1).alias('ERISAInd3'),
        df.value.substr(265, 19).alias('Filler3'),
        df.value.substr(284, 3).alias('CoverPlan4'),
        df.value.substr(287, 3).alias('BenefitCd4'),
        to_timestamp(df.value.substr(290, 0), 'yyyy-MM-dd HH:mm:ss').alias('BenefitStartDate4'),
        to_timestamp(df.value.substr(290, 0), 'yyyy-MM-dd HH:mm:ss').alias('BenefitEndDate4'),
        df.value.substr(290, 0).cast(DecimalType()).alias('CopayAmt4'),
        df.value.substr(290, 1).alias('CopayType4'),
        df.value.substr(291, 6).alias('EmpGroup4'),
        df.value.substr(297, 6).alias('ProvGroup4'),
        df.value.substr(303, 4).alias('ProvFacility4'),
        df.value.substr(307, 0).alias('SpecialPlanCd4'),
        df.value.substr(307, 1).alias('ERISAInd4'),
        df.value.substr(308, 19).alias('Filler4'),
        df.value.substr(327, 3).alias('CoverPlan5'),
        df.value.substr(330, 3).alias('BenefitCd5'),
        to_timestamp(df.value.substr(333, 0), 'yyyy-MM-dd HH:mm:ss').alias('BenefitStartDate5'),
        to_timestamp(df.value.substr(333, 0), 'yyyy-MM-dd HH:mm:ss').alias('BenefitEndDate5'),
        df.value.substr(333, 0).cast(DecimalType()).alias('CopayAmt5'),
        df.value.substr(333, 1).alias('CopayType5'),
        df.value.substr(334, 6).alias('EmpGroup5'),
        df.value.substr(340, 6).alias('ProvGroup5'),
        df.value.substr(346, 4).alias('ProvFacility5'),
        df.value.substr(350, 5).alias('SpecialPlanCd5'),
        df.value.substr(355, 1).alias('ERISAInd5'),
        df.value.substr(356, 19).alias('Filler5'),
        df.value.substr(375, 3).alias('CoverPlan6'),
        df.value.substr(378, 3).alias('BenefitCd6'),
        to_timestamp(df.value.substr(381, 0), 'yyyy-MM-dd HH:mm:ss').alias('BenefitStartDate6'),
        to_timestamp(df.value.substr(381, 0), 'yyyy-MM-dd HH:mm:ss').alias('BenefitEndDate6'),
        df.value.substr(381, 0).cast(DecimalType()).alias('CopayAmt6'),
        df.value.substr(381, 1).alias('CopayType6'),
        df.value.substr(382, 6).alias('EmpGroup6'),
        df.value.substr(388, 6).alias('ProvGroup6'),
        df.value.substr(394, 4).alias('ProvFacility6'),
        df.value.substr(398, 5).alias('SpecialPlanCd6'),
        df.value.substr(403, 1).alias('ERISAInd6'),
        df.value.substr(404, 19).alias('Filler6'),
        df.value.substr(423, 7).alias('PrimarySubscriberNum'),
        df.value.substr(430, 2).alias('PrimarySubscriberSuffix'),
        df.value.substr(432, 2).alias('DependentCd'),
        df.value.substr(434, 1).alias('MemberStatus'),
        df.value.substr(435, 2).alias('RelCd'),
        df.value.substr(437, 4).alias('UserDefined')
    )
    # df2.show()
    # columns_list=df2.columns
    # print(columns_list)
    # get the list of unique ID values ; there's probably a better way to do this, but this was quick and easy
    print(type(df2))
    TableName = "PCSecureHorizonsAZ4670MemberLoad"
    # listids = [list(x.asDict().values())[0] for x in df2.select("memberID").distinct().collect()]
    # create list of dataframes by IDs
    samples = np.arange(10, 15, 0.5).tolist()
    dfArray = df2.randomSplit(samples, 24)
    params = 'DRIVER=' + config.Dev['driver'] + ';SERVER=' + config.Dev['db_server']  + ';PORT=1433;DATABASE=' + "Eligibility" + ';UID=' + config.Dev['user'] + ';PWD=' + config.Dev['passwd']
    db_params = urllib.parse.quote_plus(params)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(db_params))
    # df is the dataframe; test is table name in which this dataframe is #inserted
    # columns_list = df2.columns
    start = time.time()
    for dataf in dfArray:
        # listofrows=np.array(dataf.select(columns_list).collect())
        # listofrows=listofrows.tolist()
        dataf = dataf.toPandas()
        dataf.to_sql(TableName, engine, index=False, if_exists="append", schema="dbo")
        # insert_query="insert into eligibility.dbo.tblHealthNewEnglandLoad values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        # executefast_sql("Eligibility","tblHealthNewEnglandLoad" , listofrows)
        print("Inserted")
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

# from pyspark import SparkConf, SparkContext
# from pyspark.sql import SparkSession
#
# # Create spark configuration object
# conf = SparkConf()
# conf.setMaster("local").setAppName("My app")
#
# # Create spark context and sparksession
# sc = SparkContext.getOrCreate(conf=conf)
# spark = SparkSession(sc)
# # set variable to be used to connect the database
# database = "Commlog"
# selectquery = "SELECT DISTINCT CLH.[LogNum],CONVERT(VARCHAR(10),[LogDate],110) AS LogDate,CONVERT(VARCHAR, [LogTime], 108) AS LogTime ,CLH.[USERID],CLH.[PROVIDERID] ,CLH.[OFFICEID],CLH.[LOGType],LT.[TypeDesc] FROM [CommLog].[dbo].[T_CommLogHeader] (NOLOCK) AS CLH INNER JOIN Commlog.dbo.T_LogType (NOLOCK) AS LT ON LT.TypeCode = CLH.LOGType INNER JOIN commlog.dbo.T_Source (NOLOCK) AS S ON S.SourceCode = CLH.LogSource INNER JOIN CommLog.dbo.Inquirydata (NOLOCK) AS ID ON ID.InquiryCode = CLH.InquiryType INNER JOIN Patient.dbo.Patient (NOLOCK) AS P ON P.ACNPatID = CLH.MPNPatID WHERE LogDate = CONVERT(VARCHAR(10),GETDATE(),101)"
# user = "SQLMON"
# password = "gJWXwpmBW6nC"
#
# # read table data into a spark dataframe
# jdbcDF = spark.read.format("jdbc") \
#     .option("url", f"jdbc:sqlserver://DBSWP0796CLS.dmzmgmt.uhc.com:1433;databaseName={database};") \
#     .option("query", selectquery) \
#     .option("user", user) \
#     .option("password", password) \
#     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
#     .load()
#
# # show the dat
# jdbcDF.show()
# jdbcDF.write \
#     .format('csv') \
#     .options(delimiter='|') \
#     .save("C:\\Users\\stiwar54\\PycharmProjects\\First\\just3.csv")