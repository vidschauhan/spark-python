# Created by vidit.singh at 09-09-2022
from pyspark.sql.functions import col

from src.utils import Paths
from src.utils.configurations.sparkConfig import SparkConfiguration

spark = SparkConfiguration.get_spark_session("Spark Pivot and Unpivot")

emp_df = spark.read.csv(Paths.base_dir() + 'employee_data.csv', header=True, inferSchema=True)

emp_data = emp_df.select(emp_df.department, emp_df.state, emp_df.salary) \
    .groupBy(emp_df.department, emp_df.state) \
    .sum("salary")
emp_data.show()

print(' *********** After PIVOT **********')
emp_data.groupBy(emp_data.department) \
    .pivot("state") \
    .sum("sum(salary)").show()

print( '******** Unpivot *****************')
# Directly any function is not available in spark. we need to use STACK(no of rows) to unpivot.
# First covert df into table to use spark sql.

emp_data.createOrReplaceTempView('table')

spark.sql(''' select department,stack(2,'CA',CA,'NY',NY) as (state,salary) from table ''').show()