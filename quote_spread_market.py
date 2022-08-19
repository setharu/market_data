from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyarrow import hdfs
from glob import *
import sys, getopt
import itertools
import datetime
from  pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys
import calendar

if (len(sys.argv)-1) == 1):
    try:
        month = sys.argv[1]
        date_check = datetime.strptime(month, '%Y%m')
        d1 = datetime.strftime(date_check, '%Y%m')
        d2 = datetime.today().strftime('%Y%m')
        print('Date passed: ',month)
        if d1 > d2 :
            print("Invalid date provided.")
            sys.exit()
    except:
        print("Invalid OrderMonth Format. Please Provide the date in the YYYYMM format.")
        sys.exit()
    
else:
    month = str(datetime.today().strftime('%Y%m'))
    date_param = (datetime.strptime(month, '%Y%m')) - relativedelta(months=1)
    month = datetime.strftime(date_param, '%Y%m')
    

spark = SparkSession.builder.appName("Ouoted Spread").enableHiveSupport().getOrCreate()
dbs = spark.sql("use mdw")
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
spark.conf.set("hive.exec.dynamic.partition","true")

level1_quote = spark.sql("select symbol, ask_price, bid_price, source_date, source_dtm from level1_quote")

market_holidays = spark.sql("select dt, short_day, equities_close_dtm from ref.market_holidays")

df = level1_quote.join(market_holidays, level1_quote.source_date == market_holidays.dt,how='left')


df = df.withColumn("source_dtm_start", F.concat(col("source_date"), lit(' 9:30:00')).cast('timestamp')).withColumn("source_dtm_end", when(((df.source_date == df.dt) & (df.short_day=='Y')),df.equities_close_dtm).otherwise( F.concat(col("source_date"), lit(' 16:00:00')).cast('timestamp')))

df = df.filter(~(df.bid_price == 0.0) & ~(df.bid_price == 0.0) & (df.ask_price > df.bid_price) ).withColumn("ask-bid",col("ask_price") - col("bid_price"))

df = df.filter(col("source_dtm").between(col('source_dtm_start'),col('source_dtm_end')))
df = df.filter(col("source_date").between((datetime.strftime(datetime.strptime((month+'01'), '%Y%m%d'), '%Y-%m-%d')),((datetime.strftime(datetime.strptime((month+str(calendar.monthrange(int(month[0:4]), int(month[4:7]))[1])), '%Y%m%d'), '%Y-%m-%d'))) ))

QuotedSpread = df.select("symbol", "ask-bid").groupBy("symbol").agg(count("symbol").alias("quote_count"),avg(col("ask-bid")).alias("quoted_spread")).orderBy('symbol', ascending=True)
QuotedSpread= QuotedSpread.withColumn("order_month",lit(month) ).withColumn("updated",lit('').cast('timestamp'))

QuotedSpread= QuotedSpread.select('symbol','quoted_spread','quote_count','updated','order_month')

QuotedSpread.write.mode("append").format('hive').option("orc.compress","snappy").partitionBy("order_month").saveAsTable("mdw.quoted_spread")