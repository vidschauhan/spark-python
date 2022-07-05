# Created by vidit.singh at 05-07-2022
from src.utils.Dataframes import get_employee_df
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, lit, col, expr, when

spark = SparkSession.builder.appName('Employee Dataframes').getOrCreate()
emp_df = get_employee_df(spark)  # getting dataframe from utils.

emp_df.show()

# Case when using expression
emp_df.withColumn('new_bonus', expr('''
CASE 
WHEN bonus IS NULL OR BONUS = "" THEN 0 
ELSE bonus 
END''')).show()

# Case when using spark API
emp_df.withColumn('bo_new', when((col('bonus').isin()) | (col('bonus') == lit("")), 0).otherwise(col('bonus'))).show()

persons = [
    (1, 1),
    (2, 13),
    (3, 18),
    (4, 60),
    (5, 120),
    (6, 0),
    (7, 12),
    (8, 160)
]

personsDF = spark.createDataFrame(persons, schema='id INT, age INT')

personsDF.withColumn('category', expr('''
CASE
WHEN age BETWEEN 0 AND 2 THEN 'New Born'
WHEN age > 2 AND age < 12 THEN 'Infant'
WHEN age > 2 AND age <= 12 THEN 'Infant'
WHEN age > 12 AND age <= 48 THEN 'Toddler'
WHEN age > 48 AND age <= 144 THEN 'Kids'
ELSE "Teenager or Adult"
END
''')).show()

personsDF.withColumn('new category',
                     when(col('age').between(0, 2), 'New Born'). \
                     when((col('age') > 2) & (col('age') <= 12), 'Infant'). \
                     when((col('age') > 12) & (col('age') <= 48), 'Toddler').
                     when((col('age') > 48) & (col('age') <= 144), 'Kid').
                     otherwise('Teenager or Adult')
                     ).show()
