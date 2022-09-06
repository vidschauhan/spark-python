# Created by vidit.singh at 18-08-2022
import pyspark
from pyspark.sql import SparkSession

from src.utils import Paths
from pyspark.sql.functions import col, lit, round

spark = SparkSession.builder.appName('Select,withColumn,filter quiz').getOrCreate()

student_df = spark.read.csv(Paths.base_dir() + 'student_data.csv', header=True, inferSchema=True)

student_marks_percentage = student_df.withColumn('totalMarks', lit(120)) \
    .withColumn('avg marks', round(col('marks') / col('totalMarks') * 100, 2))

student_marks_percentage.filter((col('avg marks') >= 80) & (col('course') == 'OOP')) \
    .select(student_marks_percentage['name'], student_marks_percentage['marks'], student_marks_percentage['avg marks']) \
    .orderBy(col('avg marks')) \
    .show(10, truncate=False)
