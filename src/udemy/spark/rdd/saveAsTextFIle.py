# Created by vidit.singh at 28-06-2022
from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Spark saveAsTextFile functions')
file_rdd = sc.textFile(Paths.base_dir() + 'sample-text-file.txt')


# Write only those data which starts from either 'a' or 'v' in file
reduced_rdd = file_rdd.flatMap(lambda x: x.split(' '))\
    .filter(lambda word: word.startswith('a') or word.startswith('v'))\
    .map(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: x + y)\
    .sortByKey(ascending=True)\
    .saveAsTextFile(Paths.output_dir()+'sample')

print(reduced_rdd)
