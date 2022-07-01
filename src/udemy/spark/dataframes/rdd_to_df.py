# Created by vidit.singh at 29-06-2022
from src.utils.configurations.sparkConfig import SparkConfiguration
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Creating DF from RDD')
spark = SparkSession.builder.appName('RDD to DF').getOrCreate()

student_rdd = sc.textFile(Paths.base_dir() + 'student_data.csv')  # Reading Text file
headers = student_rdd.first()  # getting first row
s_rdd = student_rdd.filter(lambda x: x != headers).map(lambda student: student.split(','))
# filtering first row and mapping data

header_schema = headers.split(',')  # Collecting schema columns from the first row. i.e headers.
s_rdd.toDF(header_schema).show(5, truncate=False)  # Converting to df and show output.

print('***************************************************************************')
# creating custom schema
student_schema = StructType([
    StructField('age', IntegerType(), True),
    StructField('gender', StringType(), True),
    StructField('name', StringType(), True),
    StructField('course', StringType(), True),
    StructField('roll', IntegerType(), True),
    StructField('marks', IntegerType(), True),
    StructField('email', StringType(), True)
])

# While converting existing RDD to df while inferring the custom schema we need to explicitly match the data types
# of the columns
new_rdd = s_rdd.map(lambda x: [int(x[0]), x[1], x[2], x[3], int(x[4]), int(x[5]), x[6]])
spark.createDataFrame(new_rdd, schema=student_schema).show(10, truncate=False)
