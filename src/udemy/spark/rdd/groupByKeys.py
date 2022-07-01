# Created by vidit.singh at 28-06-2022

from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Spark groupByKeys functions')
file_rdd = sc.textFile(Paths.base_dir() + 'sample-text-file.txt')

# To use groupByKey the input format must be in Tuple (keys,values).
# The groupByKeys will group the data on keys and will return as Tuple(keys,[list of values for all keys]).
# You may iterate over list of values or just print using mapValues()
flatten_rdd = file_rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, len(x))).groupByKey().mapValues(list).collect()
print(flatten_rdd)
