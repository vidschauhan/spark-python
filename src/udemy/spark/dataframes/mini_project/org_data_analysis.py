# Created by vidit.singh at 01-07-2022
from pyspark.sql import SparkSession
from src.utils import Paths
from pyspark.sql.functions import col, column, udf
from pyspark.sql.types import LongType
from pyspark.sql.functions import min, max, avg

import findspark

findspark.init()
spark = SparkSession.builder.appName('Group by Dataframes').getOrCreate()
org_df = spark.read.csv(Paths.base_dir() + 'org_data.csv', header=True, inferSchema=True)
org_df.persist()

org_df.show(10, truncate=False)
print('Total no. of Employees : ', org_df.count())

print('Total Department : ')
org_df.select(org_df['department']).distinct().show()

print('Total no. department : ', org_df.select(org_df['department']).distinct().count())

print('Total no. of emp in each department : ')
org_df.groupBy(org_df.department).count().show()

print('Total no. of emp in each state : ')
org_df.groupBy(org_df.state).count().show()

print('Total no. of emp in each state each department : ')
org_df.groupBy(org_df.state, org_df.department).count().show()

org_df.groupBy(org_df.department) \
    .agg(min(org_df.salary).alias('min salary'), max(org_df.salary).alias('max salary')) \
    .orderBy('min salary').show()

print("********************************************************************************************")

# Print the name of the employee working in NY state under FINANCE department
# whose bonus is greater than avg bonus of employee in NY state
emp_avg_bonus_rdd = org_df.filter(org_df.state == 'NY') \
    .groupBy(org_df.department).agg(avg(org_df.bonus).alias('avg_bonus')).collect()[0]['avg_bonus']

print('Avg Bonus :::: ', emp_avg_bonus_rdd)

org_df.filter((org_df.state == 'NY') & (org_df.department == 'Finance') & (org_df.salary > emp_avg_bonus_rdd)).show()


# Raise the salary of all the employee whose age is greater than 45.

def raise_sal(sal):
    return sal + 500


updated_sal = udf(raise_sal, LongType())
org_df.filter(org_df.age > 45).withColumn('udf_sal', updated_sal(org_df.salary)).show(20, truncate=False)

org_df.filter(org_df.age > 45).withColumn('updated_salary', col('salary') + 500).show(20, truncate=False)
