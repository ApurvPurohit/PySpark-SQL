import findspark
findspark.init()
from pyspark import SparkContext, SparkConf, SQLContext
import pandas as pd
appName = "PySpark SQL Server"
master = "local"
conf = SparkConf() \
    .setAppName(appName) \
    .setMaster(master)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession
df = spark.read.format("jdbc").option("url","jdbc:mysql://localhost/dbset2047").option("dbtable","xyz").option("user","appPhysicalHealth").option("password","7J=xN$?=9ZUe").load()