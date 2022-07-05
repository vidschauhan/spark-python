# Created by vidit.singh at 05-07-2022
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lpad, date_format, to_date

from src.utils import Paths

spark = SparkSession.builder.appName('Employee Dataframes').getOrCreate()
flight_data_df = spark.read.parquet(Paths.base_dir() + '\\flightmonth200801\\part-00000.parquet')
# flight_data_df.printSchema()
# flight_data_df.show(10, truncate=False)

flight_data_df.select(
    concat(
        col("Year"),
        lpad(col("Month"), 2, "0"),
        lpad(col("DayOfMonth"), 2, "0")
    ).alias('FlightDate')
).show()

# get no. of flight which were delayed on sundays
flight_data_df.select(col('isDepDelayed'), concat(
    col("Year"),
    lpad(col("Month"), 2, "0"),
    lpad(col("DayOfMonth"), 2, "0")
).alias('FlightDate')) \
    .select(col('isDepDelayed'), date_format(to_date('FlightDate', 'yyyyMMdd'), 'EEEE').alias('FlightDateFormatted')) \
    .show(10, truncate=True)

delayed_count = flight_data_df.select(col('isDepDelayed'), concat(
    col("Year"),
    lpad(col("Month"), 2, "0"),
    lpad(col("DayOfMonth"), 2, "0")
).alias('FlightDate')) \
    .filter((col('isDepDelayed') == 'YES') & (date_format(to_date('FlightDate', 'yyyyMMdd'), 'EEEE') == 'Sunday')) \
    .count()

print('No. of flights departed delayed on Sundays :: ', delayed_count)


flight_data_df. \
    withColumn("FlightDate",
               concat(col("Year"),
                      lpad(col("Month"), 2, "0"),
                      lpad(col("DayOfMonth"), 2, "0")
                     )
              ). \
    filter("""
           IsDepDelayed = 'YES' AND Cancelled = 0 AND
           date_format(to_date(FlightDate, 'yyyyMMdd'), 'EEEE') IN
               ('Saturday', 'Sunday')
           """). \
    count()