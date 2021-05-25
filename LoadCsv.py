import findspark
findspark.init()
from pyspark.sql import session
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, lit
import pyodbc
import Config as config
from datetime import datetime

loaddate = str(datetime.now())[:-4]
spark = SparkSession.builder.appName("ETL")\
        .config("spark.driver.extraClassPath","spark-mssql-connector_2.12_3.0-1.0.0-alpha")\
        .getOrCreate()


def execute_sql(db, query):
    try:
        connection = pyodbc.connect('driver={%s};server=%s;database=%s;uid=%s;pwd=%s' % (config.Dev['driver'],
                                                                                         config.Dev['db_server'], db,
                                                                                         config.Dev['user'],
                                                                                         config.Dev['passwd']))
        cursor = connection.cursor()
        print(query)
        cursor.execute(query)
        print("Executed the query")
        if "insert" in query:
            connection.commit()
        else:
            return cursor
    except Error as err:
        print(f"Error: '{err}'")


def get_loadnum():

    insert_Load = "insert into eligibility..t_eligloads values ('"+loaddate+"','MEMBERS.TXT'," \
                  "'tblHealthNewEnglandLoad','602, 604, 605')"
    execute_sql("Eligibility",  insert_Load)

    selectloadnum = "select LoadNum from eligibility..t_eligloads where FileName = 'MEMBERS.TXT' and LoadDate = '" \
                    + loaddate+"'"
    cursor = execute_sql("Eligibility", selectloadnum)
    return str(cursor.fetchone()[0])


def write_into_database(dataframe, database, table_name,  mode):
    dataframe.write \
        .format("jdbc") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("url", "jdbc:sqlserver://" + config.Dev['db_server'] + ":" + config.Dev['port'] + ";databaseName="
                + database) \
        .option("dbtable", table_name) \
        .option("user", config.Dev['user']) \
        .option("password", config.Dev['passwd']) \
        .mode(saveMode=mode) \
        .save()

def readfile():
    df = spark.read.format("Text").option("header", "True").load("MEMBER_20210310_1.txt")
    df2 = df.select(
        lit(get_loadnum()).alias('LoadNum'),
        df.value.substr(1, 11).alias('memberID'),
        df.value.substr(12, 4).alias('Suffix'),
        df.value.substr(16, 9).alias('SSN'),
        df.value.substr(25, 15).alias('AlternateID1'),
        df.value.substr(40, 15).alias('AlternateID2'),
        df.value.substr(55, 30).alias('LastName'),
        df.value.substr(85, 15).alias('FirstName'),
        df.value.substr(100, 1).alias('MI'),
        df.value.substr(101, 3).alias('Title'),
        to_timestamp(df.value.substr(104, 8), 'yyyy-MM-dd HH:mm:ss').alias('DOB'),
        df.value.substr(112, 1).alias('Gender'),
        df.value.substr(113, 1).alias('maritalstatus'),
        df.value.substr(114, 2).alias('relationtosubscriber'),
        df.value.substr(116, 30).alias('Address1'),
        df.value.substr(146, 30).alias('Address2'),
        df.value.substr(176, 15).alias('City'),
        df.value.substr(191, 2).alias('State'),
        df.value.substr(193, 9).alias('ZipCode'),
        df.value.substr(202, 10).alias('HomePhone'),
        df.value.substr(212, 4).alias('Product'),
        df.value.substr(216, 1).alias('Coverage'),
        df.value.substr(217, 18).alias('GroupNumber'),
        df.value.substr(235, 30).alias('GroupName'),
        df.value.substr(265, 8).alias('Benefit'),
        to_timestamp(df.value.substr(273, 8), 'yyyy-MM-dd HH:mm:ss').alias('OriginalEffectiveDate'),
        to_timestamp(df.value.substr(281, 8), 'yyyy-MM-dd HH:mm:ss').alias('CurrentEffectiveDate'),
        df.value.substr(289, 8).alias('TermDate'),
        df.value.substr(297, 6).alias('Copay'),
        df.value.substr(303, 4).alias('AnnualVisitLimit'),
        df.value.substr(307, 10).alias('PCPID'),
        df.value.substr(317, 30).alias('PCPName'),
        df.value.substr(347, 4).alias('ClaimSite'),
        df.value.substr(351, 1).alias('COBIndicator'),
        df.value.substr(352, 30).alias('COBName'),
        df.value.substr(382, 1).alias('Medicare'),
        df.value.substr(383, 30).alias('MiscellaneousInformation'),
        df.value.substr(413, 30).alias('MedicarePlan')
    )
    print("This is DF2:\n")
    df2.show()
    write_into_database(df2, 'Eligibility', 'dbo.tblHealthNewEnglandLoad', 'Append')


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




