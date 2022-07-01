# Created by vidit.singh at 28-06-2022
from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Spark Map functions')
file_rdd = sc.textFile(Paths.base_dir() + 'sample.txt')
f_map = file_rdd.map(lambda item: [int(it) ** 3 for it in item.split(' ')])
flat_map_rdd = f_map.flatMap(lambda x: x)

for it in flat_map_rdd.collect():
    print(it)
