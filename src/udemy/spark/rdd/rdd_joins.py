# Created by vidit.singh at 01-09-2022
from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context("RDD Joins")
order_items_rdd = sc.textFile(Paths.base_dir() + "retail_db\\order_items\part-00000")
orders_rdd = sc.textFile(Paths.base_dir() + "retail_db\\orders\part-00000")

# To join two RDDs we need to convert into [K,V] to each RDD.
orders_map = orders_rdd.map(lambda record: (record.split(',')[0], record.split(',')[2]))
order_items_map = order_items_rdd.map(lambda record: (record.split(',')[1], record.split(',')[4]))
orders_joined_data = orders_map.join(order_items_map)

print(orders_joined_data.takeOrdered(5))
