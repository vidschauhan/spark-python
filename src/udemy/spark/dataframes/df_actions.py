# Created by vidit.singh at 30-06-2022

from pyspark.sql import SparkSession
from src.utils import Paths
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('Creating Dataframes').getOrCreate()
student_df = spark.read.csv(Paths.base_dir() + 'student_data.csv', header=True, inferSchema=True)
student_df.cache()
print('Female count : ', student_df.filter(col('gender') == 'Female').count())
print('Female distinct count : ', student_df.filter(col('gender') == 'Female').distinct().count())

print('Distinct gender count : ', student_df.select('gender').distinct().count())

student_df.dropDuplicates(['gender']).show()

# will check the unique combination for gender and course. If the same combination repeats it will drop.
student_df.dropDuplicates(['gender', 'course']).show()
