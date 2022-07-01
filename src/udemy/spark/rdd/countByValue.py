# Created by vidit.singh at 28-06-2022

from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Spark groupByKeys functions')
file_rdd = sc.textFile(Paths.base_dir() + 'sample-text-file.txt')

# CountByValue is an Action which returns the key with its count as dictionary{key,count}
count_by_val_rdd = file_rdd.flatMap(lambda x: x.split(' ')).countByValue()
print(count_by_val_rdd)