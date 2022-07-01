# Created by vidit.singh at 30-06-2022

from pyspark.sql import SparkSession
from src.utils import Paths
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('Creating Dataframes').getOrCreate()
student_df = spark.read.csv(Paths.base_dir() + 'student_data.csv', header=True, inferSchema=True)

# student_df.filter('course' == 'DB').show(10, truncate=False)
student_df.filter(col('course') == 'DB').show(2, truncate=False)
student_df.filter((col('course') == 'DB') | (col('marks') > 50)).show(5, truncate=False)
student_df.filter((student_df.course == 'DB') & (student_df.marks > 50)).show(5, truncate=False)
courses = ['DB', 'DSA', 'Cloud']
student_df.filter((student_df.course.isin(courses)) & (student_df.marks > 50)).show(5, truncate=False)

print('***************************************************************')
student_df.filter(col('name').like('%Pa%')).show(2, truncate=False)
student_df.filter(col('name').contains('Pa')).show(2, truncate=False)