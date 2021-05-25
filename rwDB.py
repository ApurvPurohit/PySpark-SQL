import findspark
findspark.init()
from pyspark import SparkContext, SparkConf, SQLContext
logFile = "C:\SPARKMEDIA\spark-2.4.7-bin-hadoop2.7\README.md"
sc = SparkContext("local", "first app")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
