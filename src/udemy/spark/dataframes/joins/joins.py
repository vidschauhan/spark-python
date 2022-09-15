# Created by vidit.singh at 08-09-2022
from src.utils.configurations.sparkConfig import SparkConfiguration

spark = SparkConfiguration.get_spark_session("Spark joins in depth")

df1 = spark.createDataFrame([(1, 'Vidit'), (2, 'Vansh'), (3, 'Vanshika')], schema=['empId', 'empName'])
df2 = spark.createDataFrame([(2, 'India'), (4, 'USA')], schema=['empId', 'country'])

df1.show()
df2.show()

# Semi-joins perform better than inner, Try to use wherever possible.
# by default inner join is applied.

print('Inner join')
df1.join(df2, on=df1.empId == df2.empId, how='inner').show()

print('Left join')
df1.join(df2, on=df1.empId == df2.empId, how='left').show()

print('Right join')
df1.join(df2, on=df1.empId == df2.empId, how='right').show()

print('Full join')
df1.join(df2, on=df1.empId == df2.empId, how='full').show()


print('cross join')
#df1.join(df2, on=df1.empId == df2.empId, how='crossjoin').show()
df1.crossJoin(df2).show()

print("Left anti join :: Note : can't select column from second df")
print('Picks non matching elements only and columns from left table only.')
df1.join(df2, on=df1.empId == df2.empId, how='leftanti').show()

print('Same as inner join but will select only left table columns. Note : Faster than inner joins')
df1.join(df2, on=df1.empId == df2.empId, how='leftsemi').show()

# Self join

print('Self join')

employee = spark.createDataFrame([(1,'Vidit',3),(2,'Vansh',1),(3,'Priya',4),(4,'Nina',2)],schema=['empId','empName','managerId'])

from pyspark.sql.functions import col
employee.alias('e1').join(employee.alias('e2'),on = col('e1.managerId') == col('e2.empId'),how='inner')\
    .select(col('e1.empId'),col('e1.empName'),col('e2.empName').alias('managerName')).show()