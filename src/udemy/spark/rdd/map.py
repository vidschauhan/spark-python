# Created by vidit.singh at 28-06-2022
from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths


def square(item):
    num_list = []
    for its in item.split(' '):
        num = int(its)
        num_list.append(num ** 2)
    return num_list


def cube(item):
    return [int(it) ** 3 for it in item.split(' ')]


sc = SparkConfiguration.get_spark_context('Spark Map functions')
file_rdd = sc.textFile(Paths.base_dir() + 'sample.txt')
f = file_rdd.map(cube)
for it in f.collect():
    print(it)
