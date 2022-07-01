# Created by vidit.singh at 30-06-2022
from pyspark.sql import SparkSession
from src.utils import Paths
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('Creating Dataframes').getOrCreate()
student_df = spark.read.csv(Paths.base_dir() + 'student_data.csv', header=True, inferSchema=True)

student_df.groupBy('gender').count().show()
student_df.groupBy('gender').sum('marks').show()
student_df.groupBy('gender').max('marks').show()
student_df.filter(student_df.course == 'DB').groupBy('gender').max('marks').show()
student_df.groupBy('gender', 'course').sum('marks').show()  # Multiple grouping columns

print('*********************** Multiple aggregation at Once ********************************')
from pyspark.sql.functions import sum, count, avg, max, min, mean

student_df.withColumn('marks', col('marks') + 10) \
    .groupBy('gender').agg(count('*').alias('count'),
                           min('marks').alias('min_marks'),
                           max('marks').alias('max_marks'),
                           sum('marks'), avg('marks').alias('average_marks'),
                           mean('marks').alias('mean')).show()
