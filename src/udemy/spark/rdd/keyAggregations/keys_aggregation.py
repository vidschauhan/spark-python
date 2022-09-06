# Created by vidit.singh at 01-09-2022

# Key Aggregations
# groupByKey : Can't use combiner. Hence, avoid as much as possible as shuffling required.
# aggregateByKey :  Combiner can be used.
# reduceByKey : Combiner can be used.
# countByKey : No Shuffling required. Can use without performance issue.

from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context("RDD Joins")
order_items_rdd = sc.textFile(Paths.base_dir() + "retail_db\\order_items\part-00000")
order_items_map = order_items_rdd.map(lambda record: (record.split(',')[2], float(record.split(',')[4]))) \
    .groupByKey().mapValues(sum)
print(order_items_map.take(10))

##### reduceByKey #######
from operator import add

data = [('a', 1), ('b', 1), ('a', 1)]
data_rdd = sc.parallelize(data)
da = data_rdd.reduceByKey(add).collect()  # Combiner is used as intermediate result.
print(da)

####### aggregateByKey #######
