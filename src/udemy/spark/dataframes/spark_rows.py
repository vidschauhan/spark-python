# Created by vidit.singh at 07-09-2022
from src.utils.configurations.sparkConfig import SparkConfiguration

spark = SparkConfiguration.get_spark_session("Spark row object")

# Rows can be created in 2 ways...

from pyspark.sql import Row

# List of row object to create dataframe.
rows = [Row(name='Vidit', age=31), Row(name='Vansh', age=2), Row(name='Vanshika', age=12)]

rows_rdd = spark.sparkContext.parallelize(rows)

for item in rows_rdd.collect():
    print(item)
    print('Vidit' in item.name)

# creating df from rdd
row_df = rows_rdd.toDF()
row_df.show()

Person = Row('name', 'number')
p1 = Person('Vidit', 8699203041)
p2 = Person('Vanshu', 872232223)

per_rdd = spark.sparkContext.parallelize([p1 + p2])
for i in per_rdd.take(2): print(i)

per_schema = ['Naam', 'Num']
per_df = per_rdd.toDF(schema=per_schema)
per_df.show()
per_df.printSchema()

print(p1.count('Vidit'))
print(p1.index('Vansh'))
print(p1.asDict())
