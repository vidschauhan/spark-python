# Created by vidit.singh at 01-07-2022
from pyspark.sql import SparkSession
from src.utils import Paths
import findspark

findspark.init()

spark = SparkSession.builder.appName('Udf').getOrCreate()
employee_df = spark.read.csv(Paths.base_dir() + 'employee_data.csv', header=True, inferSchema=True)

employee_rdd = employee_df.rdd
print(employee_rdd.filter(lambda x: x['department'] == 'Sales').collect())
