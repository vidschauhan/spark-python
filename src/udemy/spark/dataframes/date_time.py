# Created by vidit.singh at 03-07-2022
print('***************** Time Manipulation *********************')
from pyspark.sql.functions import lit, to_date, to_timestamp, months_between, add_months, trunc, \
    date_trunc, date_add, date_sub, current_timestamp, current_date, datediff, year, month, weekofyear, dayofyear, \
    dayofmonth, dayofweek, col, date_format, unix_timestamp, from_unixtime

from pyspark.sql import SparkSession
import findspark

findspark.init()
spark = SparkSession.builder.appName('Employee Dataframes').getOrCreate()

lis = [("X",)]
df = spark.createDataFrame(lis).toDF("dummy")

df.select(current_timestamp()).alias('current time').show()
df.select(to_date(lit('20210228'), 'yyyyMMdd').alias('to_date')).show()
df.select(to_timestamp(lit('20210228 1725'), 'yyyyMMdd HHmm').alias('to_timestamp')).show()

df.select(
    current_date().alias('current_date'),
    year(current_date()).alias('year'),
    month(current_date()).alias('month'),
    weekofyear(current_date()).alias('weekofyear'),
    dayofyear(current_date()).alias('dayofyear'),
    dayofmonth(current_date()).alias('dayofmonth'),
    dayofweek(current_date()).alias('dayofweek')
).show() #yyyy-MM-dd

df.persist()
print('****************** date time add/sub *********************')
date_times = [("2014-02-28", "2014-02-28 10:00:00.123"),
              ("2016-02-29", "2016-02-29 08:08:08.999"),
              ("2017-10-31", "2017-12-31 11:59:59.123"),
              ("2019-11-30", "2019-08-31 00:00:00.000")
              ]
date_times_df = spark.createDataFrame(date_times, schema="date STRING, time STRING")

date_times_df.withColumn('date_add', date_add('time', 10)) \
    .withColumn('date_add', date_sub('time', 10)) \
    .withColumn('date_diff_time', datediff('time', current_timestamp())) \
    .withColumn('date_diff_date', datediff('date', current_date())).show(10, truncate=False)

date_times_df. \
    withColumn("months_between_date", months_between(current_date(), "date"), 2). \
    withColumn("months_between_time", months_between(current_timestamp(), "time"), 2). \
    withColumn("add_months_date", add_months("date", 3)). \
    withColumn("add_months_time", add_months("time", 3)). \
    show(truncate=False)
print('******************************************************************')
print('********************** Date trunc ********************************')

# To get the first day of the month and year, Note - for month 'MM' in caps and for year 'yy' in lower case.
date_times_df. \
    withColumn("date_trunc", trunc("date", "MM")). \
    withColumn("time_trunc", trunc("time", "yy")). \
    withColumn("date_trunc", date_trunc('MM', "date")). \
    withColumn("time_trunc", date_trunc('yy', "time")). \
    withColumn("date_dt", date_trunc("HOUR", "date")). \
    withColumn("time_dt", date_trunc("HOUR", "time")). \
    withColumn("time_dt1", date_trunc("dd", "time")). \
    show(truncate=False)

print('*********************************************************')
print('************************ to_date to_timeStamp ********************************')

# year and day of year to standard date
df.select(to_date(lit('2021061'), 'yyyyDDD').alias('to_date')).show()
df.select(to_date(lit('02/03/2021'), 'dd/MM/yyyy').alias('to_date')).show()
df.select(to_date(lit('02-03-2021'), 'dd-MM-yyyy').alias('to_date')).show()
df.select(to_date(lit('02-Mar-2021'), 'dd-MMM-yyyy').alias('to_date')).show()

df.select(to_timestamp(lit('02-Mar-2021 17:30:15'), 'dd-MMM-yyyy HH:mm:ss').alias('to_date')).show()

date_times_df. \
    withColumn('to_date', to_date(col('date').cast('string'), 'yyyyMMdd')). \
    withColumn('to_timestamp', to_timestamp(col('time'), 'dd-MMM-yyyy HH:mm:ss.SSS')). \
    show(truncate=False)


print('************************ date_format Function ********************************')
date_times_df. \
    withColumn("date_ym", date_format("date", "yyyyMM")). \
    withColumn("time_ym", date_format("time", "yyyyMM")). \
    show(truncate=False)

date_times_df. \
    withColumn("date_desc", date_format("date", "MMMM d, yyyy")). \
    show(truncate=False)

date_times_df. \
    withColumn("day_name_abbr", date_format("date", "EE")). \
    show(truncate=False)

date_times_df. \
    withColumn("day_name_full", date_format("date", "EEEE")). \
    show(truncate=False)

print('************************ Unix timestamp ********************************')
datetime_1 = [(20140228, "2014-02-28", "2014-02-28 10:00:00.123"),
                     (20160229, "2016-02-29", "2016-02-29 08:08:08.999"),
                     (20171031, "2017-10-31", "2017-12-31 11:59:59.123"),
                     (20191130, "2019-11-30", "2019-08-31 00:00:00.000")
                ]

datetimesDF = spark.createDataFrame(datetime_1).toDF("dateid", "date", "time")

datetimesDF. \
    withColumn("unix_date_id", unix_timestamp(col("dateid").cast("string"), "yyyyMMdd")). \
    withColumn("unix_date", unix_timestamp("date", "yyyy-MM-dd")). \
    withColumn("unix_time", unix_timestamp("time")). \
    show()

unixtimes = [(1393561800, ),
             (1456713488, ),
             (1514701799, ),
             (1567189800, )
            ]

unixtimesDF = spark.createDataFrame(unixtimes).toDF("unixtime")

unixtimesDF. \
    withColumn("date", from_unixtime("unixtime", "yyyyMMdd")). \
    withColumn("time", from_unixtime("unixtime")). \
    show()
#yyyyMMdd