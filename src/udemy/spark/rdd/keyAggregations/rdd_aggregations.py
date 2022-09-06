# Created by vidit.singh at 01-09-2022
from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context("RDD Joins")
order_items_rdd = sc.textFile(Paths.base_dir() + "retail_db\\order_items\part-00000")
orders_rdd = sc.textFile(Paths.base_dir() + "retail_db\\orders\part-00000")

print('********* Orders *********')
for item in orders_rdd.take(5):
    print(item)
print('***************************\n')
print('********* Order Items *********')

for item in order_items_rdd.take(5):
    print(item)

print('***************************\n')

total_qty = order_items_rdd.filter(lambda record: int(record.split(',')[1]) <= 10) \
    .map(lambda record: int(record.split(',')[3])).reduce(lambda x, y: x + y)

print(f'total quantity of order 1-10 {total_qty}')
