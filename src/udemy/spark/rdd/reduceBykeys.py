# Created by vidit.singh at 28-06-2022

from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Spark groupByKeys functions')
file_rdd = sc.textFile(Paths.base_dir() + 'sample-text-file.txt')

# The reduceByKeys will also take input as Tuple(keys,values) but will output as combined data as provided by
# expressions.
reduced_rdd = file_rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()
print(reduced_rdd)
