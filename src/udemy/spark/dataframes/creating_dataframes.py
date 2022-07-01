# Created by vidit.singh at 29-06-2022

from pyspark.sql import SparkSession
from src.utils import Paths

spark = SparkSession.builder.appName('Creating Dataframes').getOrCreate()
spark.read.csv(Paths.base_dir() + 'student_data.csv', header=True, inferSchema=True).show(10,truncate=False)
# or   spark.read.option('header',True).csv(Paths.base_dir() + 'student_data.csv').show()
# df.printSchema()