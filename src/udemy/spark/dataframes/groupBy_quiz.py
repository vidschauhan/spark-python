# Created by vidit.singh at 30-06-2022
from pyspark.sql import SparkSession
from src.utils import Paths
from pyspark.sql.functions import col
from pyspark.sql.functions import min, max, avg

spark = SparkSession.builder.appName('Group by Dataframes').getOrCreate()
student_df = spark.read.csv(Paths.base_dir() + 'student_data.csv', header=True, inferSchema=True)

student_df.cache()
student_df.groupBy(col('course')).count().show()
student_df.groupBy('gender', 'course').count().show()
student_df.groupBy('gender', 'course').sum('marks').show()
student_df.groupBy('age', col('course')).agg(min('marks'), max('marks'), avg('marks').alias('avg_marks'))\
    .filter(col('avg_marks') > 60.0).orderBy(col('avg_marks')).show()
