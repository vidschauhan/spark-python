# Created by vidit.singh at 30-06-2022

from src.utils.configurations.sparkConfig import SparkConfiguration
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Creating DF from RDD')
spark = SparkSession.builder.appName('RDD to DF').getOrCreate()

student_rdd = sc.textFile(Paths.base_dir() + 'student_data.csv')  # Reading Text file
headers = student_rdd.first()  # getting first row
s_rdd = student_rdd.filter(lambda x: x != headers).map(lambda student: student.split(','))
# filtering first row and mapping data

header_schema = headers.split(',')  # Collecting schema columns from the first row. i.e headers.
student_df = s_rdd.toDF(header_schema)
student_df.cache()
print('*********** All Columns ********* ', student_df.columns)
student_df.select(student_df.columns).show(2, truncate=False)
student_df.select('*').show(2, truncate=False)
student_df.select('age', 'gender').show(2, truncate=False)
student_df.select(student_df.age, student_df.marks).show(2, truncate=False)

from pyspark.sql.functions import col

student_df.select(col('age'), col('gender')).show(2, truncate=False)
student_df.select(student_df.columns[:3]).show(2, truncate=False)
student_df.select(student_df.columns[2:5]).show(2, truncate=False)

# With Columns -> used to manipulating columns
# Here we are going to change the data typeof a single column using withColumns.

print('******************* WithColumns ********************')
print('Old schema')
student_df.printSchema()

new_df = student_df.withColumn('roll', col('roll').cast('String'))
print('New schema')
new_df.printSchema()

print('******************* Manipulating columns -> WithColumns ********************')
# adding marks to the existing rows.
student_df.withColumn('marks', col('marks') + 10).show(2, truncate=False)
# Creates a new column but existing# col remains.
student_df.withColumn('updated marks', col('marks') + 20).show(2, truncate=False)
# creates a new columns and fills default literal value for all rows.
student_df.withColumn('new_column', lit('new_col_value')).show(2, truncate=False)

print('******************* Manipulating multiple columns -> WithColumns ********************')
# In this scenario second withColumns will take the reference from current data frame created in this step by
# firstWitchCol -> eg - first transformation (marks = 40 - 10 = 30) -> second transformation(marks = 30 + 20 = 50 )
student_df.withColumn('marks', col('marks') - 10) \
    .withColumn('updated marks', col('marks') + 20) \
    .withColumn('country', lit('India')).show(2, truncate=False)

print('******************* Renaming columns -> WithColumnRenamed ********************')
# If you are renaming a column which doesn't exist it won't throw error.
student_df.withColumnRenamed('gender', 'sex').withColumnRenamed('roll', 'roll number') \
    .sort('name', 'roll number', ascending=[0, 1]).show(2, truncate=False)
# Renamed while reading data from dataframe
student_df.select(col('name')).alias('student name').show(2, truncate=False)

print('****************************** Drop columns *************************************')
student_df.select('*').drop('email').show(2, truncate=False)
