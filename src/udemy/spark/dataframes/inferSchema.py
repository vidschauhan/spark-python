# Created by vidit.singh at 29-06-2022

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.utils import Paths

spark = SparkSession.builder.appName('Inferring Schema').getOrCreate()

# Creating custom schema for spark dataframes.
student_schema = StructType([
    StructField('age', IntegerType(), True),
    StructField('gender', StringType(), True),
    StructField('name', StringType(), True),
    StructField('course', StringType(), True),
    StructField('roll', StringType(), True),
    StructField('marks', IntegerType(), True),
    StructField('email', StringType(), True)
])

df = spark.read.csv(Paths.base_dir() + 'student_data.csv', header=True, schema=student_schema)
df.printSchema()
