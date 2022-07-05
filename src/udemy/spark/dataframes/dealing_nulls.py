# Created by vidit.singh at 05-07-2022
from src.utils.Dataframes import get_employee_df
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, lit, col, expr

spark = SparkSession.builder.appName('Employee Dataframes').getOrCreate()
emp_df = get_employee_df(spark)  # getting dataframe from utils.

emp_df.show()

# puts 0 if the field is null. Note : put lit() as it will try to find the column with name as 0
emp_df.withColumn('bonus', coalesce('bonus', lit(0))).show()

# casting null/empty string to int will result in null value, which then can be handled by coalesce
emp_df.withColumn('bonus1', coalesce(col('bonus').cast('int'), lit(0))).show()

print(' ************************** Sql style null handling using expr() **************')
emp_df.withColumn('bonus', expr("nvl(bonus, 0)")).show()

# nullif will convert '' empty string to null, then nvl will convert them as 0.
emp_df.withColumn('bonus', expr("nvl(nullif(bonus, ''), 0)")).show()

emp_df.withColumn('payment', col('salary') + (col('salary') * coalesce(col('bonus').cast('int'), lit(0)) / 100)).\
    show()