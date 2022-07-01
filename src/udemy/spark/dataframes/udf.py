# Created by vidit.singh at 30-06-2022
from pyspark.sql import SparkSession
from src.utils import Paths
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import findspark
findspark.init()

spark = SparkSession.builder.appName('Creating Dataframes').getOrCreate()
student_df = spark.read.csv(Paths.base_dir() + 'student_data.csv', header=True, inferSchema=True)


def inc_marks(marks):
    return int(marks) + 10


new_marks = udf(lambda x: inc_marks(x), IntegerType())

student_df.withColumn('updated_marks', new_marks(student_df['marks'])).show(10, truncate=False)
