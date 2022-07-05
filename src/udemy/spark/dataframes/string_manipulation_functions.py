# Created by vidit.singh at 03-07-2022
from pyspark.sql import SparkSession
from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils.schema import custom_schema
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Orders rdd')
spark = SparkSession.builder.appName('Creating Dataframes').getOrCreate()

# We may read data into RDD and then convert into dataframe but in-efficient if there are more cols. you need to cast.

# orders_rdd = sc.textFile(Paths.base_dir() + '\\retail_db\\orders\\part-00000') \
#     .map(lambda x: (int(x.split(',')[0]),
#                     x.split(',')[1],
#                     int(x.split(',')[2]),
#                     x.split(',')[3]))
# orders_df = orders_rdd.toDF(schema)
# orders_df.printSchema()
# orders_df.show(10, truncate=False)
# print(orders_rdd.collect())
# orders_df = spark.read.text(Paths.base_dir() + '\\retail_db\\orders\\part-00000')


s_df = spark.read.schema(custom_schema['orders']).csv(Paths.base_dir() + '\\retail_db\\orders\\part-00000', header=False)
s_df.show(10, truncate=False)
s_df.printSchema()

# pyspark.sql.DataFrame.selectExpr() is similar to select() with the only difference being that it accepts SQL
# expressions (in string format) that will be executed. Again, this expression will return a new DataFrame out of the
# original based on the input provided.

s_df.selectExpr('order_id', 'order_date as o_date', 'order_status as status') \
    .orderBy('status').show(10, truncate=False)
