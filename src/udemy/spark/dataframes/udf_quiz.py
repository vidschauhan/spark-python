# Created by vidit.singh at 01-07-2022
from pyspark.sql import SparkSession
from src.utils import Paths
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType,DoubleType
import findspark

findspark.init()

spark = SparkSession.builder.appName('Udf').getOrCreate()
employee_df = spark.read.csv(Paths.base_dir() + 'employee_data.csv', header=True, inferSchema=True)


def new_salary(salary, bonus, state):
    inc = 0
    if state == 'NY':
        inc = salary * 10 / 100 + bonus * 5 / 100
    else:
        inc = salary * 12 / 100 + bonus * 2 / 100
    return salary + inc


incr_sal = udf(lambda sal, bon, st: new_salary(sal, bon, st),DoubleType() )

employee_df.withColumn('Increased Salary',
                       incr_sal(employee_df['salary'], employee_df['bonus'], employee_df['state'])).orderBy('Increased Salary').show()
