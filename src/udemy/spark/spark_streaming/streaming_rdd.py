# Created by vidit.singh at 03-07-2022

from src.utils.configurations.sparkConfig import SparkConfiguration
from pyspark.streaming import StreamingContext
from pyspark import SparkConf,SparkContext
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Spark Streaming')
ssc = StreamingContext(sc, 10)  # create Streaming context from spark context and reads stream every 1 second.

d_stream = ssc.textFileStream(Paths.base_dir() + '\\streaming\\')
#data = d_stream.flatMap(lambda word: word.split(' ')).countByValue()
d_stream.pprint()
ssc.start()
ssc.awaitTermination(1000)

#from pyspark.sql import SparkSession
#spark = SparkSession.builder.appName("Sparking Streaming DF").getOrCreate()
#word = spark.readStream.text(Paths.base_dir() + '\\streaming\\')
#word = word.groupBy("value").count()
# word.writeStream.format("console").outputMode("complete").start()

# COMMAND ----------

#print(word)