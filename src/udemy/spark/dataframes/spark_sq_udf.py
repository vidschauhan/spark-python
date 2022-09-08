# Created by vidit.singh at 07-09-2022
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from src.utils.configurations.sparkConfig import SparkConfiguration


# defining udf
@udf(returnType=StringType())
def get_cap(name):
    return name.upper()


spark = SparkConfiguration.get_spark_session("Spark Sql UDFs")
df = spark.createDataFrame([('12', 'Vidit'), ('15', 'Vansh')], schema=['age', 'name'])
df.show()

df.select(df.name, get_cap(df.name).alias('cap_name')).show()

# Note : For Spark sql : You need to register UDF first then use with spark sql.
# Not shared between other spark session. Limited to single session b
df.createOrReplaceTempView('persons')
spark.udf.register('convert_cap', get_cap)  # registering to spark sql before using it.
spark.sql(""" select *,convert_cap(name) as capital_name from persons """).show()
