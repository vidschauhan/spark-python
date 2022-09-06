# Created by vidit.singh at 02-09-2022
from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context("RDD Union")
order_items_rdd = sc.textFile(Paths.base_dir() + "retail_db\\order_items\part-00000")
orders_rdd = sc.textFile(Paths.base_dir() + "retail_db\\orders\part-00000")

# Get all the customers who have ordered in July and August month

# Union will return all the records in both the RDD along with duplicates one. The structure must be identical of
# both the RDDs in order to perform Union.
july_orders = orders_rdd.filter(lambda x: x.split(',')[1].split('-')[1] == '07').map(lambda x: x.split(',')[2])
aug_orders = orders_rdd.filter(lambda x: x.split(',')[1].split('-')[1] == '08').map(lambda x: x.split(',')[2])

print(f'union ::  {july_orders.union(aug_orders).count()}')

# Intersection only considers the common values and rejects everything else.

print(f'intersection ::  {july_orders.intersection(aug_orders).count()}')

# Subtract : A - B means we get output as all the element present in the A (left) Rdd not in B (Right RDD).
# Just subtract A - B as math to get the output.

l1 = [1, 2, 3, 3, 4, 5, 5]
l2 = [1, 6, 5, 0, 4, 8, 9]
rdd1 = sc.parallelize(l1)
rdd2 = sc.parallelize(l2)

for item in rdd1.subtract(rdd2).collect():
    print(item)
