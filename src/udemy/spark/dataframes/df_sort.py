# Created by vidit.singh at 30-06-2022
from pyspark.sql import SparkSession
from src.utils import Paths
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('Creating Dataframes').getOrCreate()
student_df = spark.read.csv(Paths.base_dir() + 'student_data.csv', header=True, inferSchema=True)

# To use sort and order by the datatype of columns must be of Integer type. We may use some other ways to sort other col
student_df.sort('marks','age').show(10,truncate=False)
student_df.sort(student_df.marks.asc(),student_df.age.desc()).show(10,truncate=False)
student_df.orderBy(student_df.roll.asc(),student_df.age.desc()).show(10,truncate=False)
student_df.printSchema()
