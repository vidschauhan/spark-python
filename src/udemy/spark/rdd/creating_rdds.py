# Created by vidit.singh at 28-06-2022
from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Read File')
file_rdd = sc.textFile(Paths.base_dir() + 'single_document.json')
l = file_rdd.collect()

for item in l:
    print(item)
