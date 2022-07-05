# Created by vidit.singh at 03-07-2022
from src.utils.Dataframes import get_employee_df
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lpad, rpad

spark = SparkSession.builder.appName('Employee Dataframes').getOrCreate()
emp_df = get_employee_df(spark)  # getting dataframe from utils.

emp_df.show(10, truncate=False)
emp_df.persist()
empFixedDF = emp_df.select(
    concat(
        lpad("employee_id", 5, "0"),
        rpad("first_name", 10, "-"),
        rpad("last_name", 10, "-"),
        lpad("salary", 10, "0"),
        rpad("nationality", 15, "-"),
        rpad("phone_number", 17, "-"),
        "ssn"
    ).alias("employee")
)

empFixedDF.show(10, truncate=False)
