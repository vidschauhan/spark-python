# Created by vidit.singh at 28-06-2022
from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Spark Map functions')
file_rdd = sc.textFile(Paths.base_dir() + 'sample.txt')
filtered_rdd = file_rdd\
    .map(lambda item: [int(it) ** 3 for it in item.split(' ')])\
    .flatMap(lambda x: x)\
    .filter(lambda x: x % 2 == 0).collect()

print(filtered_rdd)
