# Created by vidit.singh at 27-06-2022
from pyspark import SparkContext
from pyspark.sql import SparkSession
import findspark
findspark.init()  # To solve the pyspark issue.

sc = SparkContext(master="local", appName="Loading Text files to DF").getOrCreate()
spark = SparkSession.builder \
    .master("local") \
    .appName("Word Count") \
    .getOrCreate()

company_data = spark.read.parquet('D:\\data\\flightmonth200801\\part-00000.parquet')
company_data.show(10,truncate=False)