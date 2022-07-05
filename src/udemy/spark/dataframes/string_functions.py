# Created by vidit.singh at 03-07-2022

from pyspark.sql import SparkSession
from src.utils.schema import custom_schema
from src.utils import Paths
from pyspark.sql.functions import col
from pyspark.sql.functions import lower, initcap, substring, lit

spark = SparkSession.builder.appName('Creating Dataframes').getOrCreate()
orders_df = spark.read.schema(custom_schema['orders']).csv(Paths.base_dir() + '\\retail_db\\orders\\part-00000',
                                                           header=False)

orders_df.groupBy(col('order_status').alias('status')).count().show()

orders_df.withColumn('l_status', lower(col('order_status'))) \
    .withColumn('init_status', initcap(col('order_status'))).show()
orders_df.cache()
orders_df.select(substring(lit(col('order_status')).alias('sub_strings'), 2, 5)).show()
