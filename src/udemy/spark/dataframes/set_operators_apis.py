# Created by vidit.singh at 08-09-2022

from src.utils.configurations.sparkConfig import SparkConfiguration

spark = SparkConfiguration.get_spark_session("Spark set operators")

df1 = spark.range(10)
df2 = spark.range(5, 10)

df_union = df1.union(df2)
df_union_all = df1.unionAll(df2)

print('***************** UNION & UNION ALL *******************')
# Both behave in same way. They both produce duplicates.
df_union.show()
df_union_all.show()

print('***************** UNION BY NAME *******************')

df_1 = spark.createDataFrame(data=[('a', 1), ('b', 2)], schema=['col1', 'col2'])
df_2 = spark.createDataFrame(data=[(1, 'a'), (2, 'b')], schema=['col2', 'col1'])

df_1.union(df_2).show()  # just union on the basis of positions.
df_1.unionByName(df_2).show()  # Unions on same column name.

print('***************** INTERSECT & INTERSECT ALL *******************')

df_x = spark.createDataFrame(data=[('a', 1), ('a', 1), ('b', 2)], schema=['col1', 'col2'])
df_y = spark.createDataFrame(data=[('a', 1), ('b', 1), ('c', 2)], schema=['col1', 'col2'])

df_x.intersect(df_y).show()  # shows common element by removing duplicates
df_x.intersectAll(df_y).show()  # shows common element & retains  duplicates

print('***************** except All *******************')

df1.exceptAll(df2).show()  # substract df1 - df2
