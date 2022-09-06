# Created by vidit.singh at 17-08-2022

from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Spark saveAsTextFile functions')

lis = [(2, ('t', 'x')), (3, ('xs', 'aw')), (6, ('tsd', 'xwq')), (34, ('fsd', 'xqe')), (11, ('tdw', 'x')),
       (31, ('eda', 'xa')), (92, ('csd', 'as')), (23, ('cs', 'xasa'))]
rdd = sc.parallelize(lis)
print(f'no. of partitions :: {rdd.getNumPartitions()}')

# Repartition data with custom function and sorted.
rdd_partitioned = rdd.repartitionAndSortWithinPartitions(2, lambda x: x % 2 == 0, ascending=True)
print(rdd_partitioned.glom().collect())


file_rdd = sc.textFile(Paths.base_dir() + 'sample-text-file.txt')

# Write only those data which has length between 3 and 6.
# repartition is used to increase or decrease the number of partitions. while Coalesce is only used to decrease.
# repartition will equally partition the data but in coalesce it is not guaranteed.
# Repartition does a lot of shuffle hence avoid if not necessary or avoid at last stage. Coalesce does less shuffling.
reduced_rdd = file_rdd.flatMap(lambda x: x.split(' ')) \
    .filter(lambda word: 3 <= len(word) >= 6) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortByKey(ascending=True) \
    .repartition(5) \
    .saveAsTextFile(Paths.output_dir() + '5partitions')

print(reduced_rdd)

# To increase the number of partition using coalesce use shuffle=True, which will behave like repartition.
rdd_partitioned.coalesce(6,shuffle=True)
