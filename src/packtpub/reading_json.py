# Created by vidit.singh at 24-08-2022
from pyspark.sql import SparkSession
from src.utils.Paths import base_dir
from pyspark.sql.types import ArrayType, StringType, IntegerType, FloatType, BooleanType, DateType, StructType, \
    StructField
from pyspark.sql.functions import col, concat_ws, expr, round, year

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('first_name', StringType(), True),
    StructField('last_name', StringType(), True),
    StructField('fav_movies', ArrayType(StringType()), True),
    StructField('salary', FloatType(), True),
    StructField('image_url', StringType(), True),
    StructField('date_of_birth', DateType(), True),
    StructField('active', BooleanType(), True)
])
base_path = f'{base_dir()}\\resources\\Section4Resources\\persons.json'
spark = SparkSession.builder.appName('reading json with multiple data types').getOrCreate()
persons_df = spark.read.json(base_path, schema, multiLine=True)
persons_df.show(10, truncate=False)

persons_df.select(expr('id'), expr('first_name')).show(5, truncate=False)
persons_df.select(col('id'), col('first_name')).show(5, truncate=False)

# concat with spaces
persons_df.select(concat_ws(" ", col('first_name'), col('last_name')).alias('full_name')
                  , round(expr('salary * 0.1 + salary'), 2).alias('increased_sal')) \
    .show(5, truncate=False)

persons_df.select(col('first_name'), year(col('date_of_birth')).alias('year'), col('active')) \
    .dropDuplicates(['year', 'active']).orderBy('year', 'first_name').show()
