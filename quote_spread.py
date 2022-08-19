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

spark = SparkSession.builder.appName("Ouoted Spread").enableHiveSupport().getOrCreate()
dbs = spark.sql("use mdw")
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
spark.conf.set("hive.exec.dynamic.partition","true")

level1_quote = spark.sql("select symbol, ask_price, bid_price, source_date, source_dtm from level1_quote")

month = str(datetime.today().strftime('%Y%m'))
date_param = (datetime.strptime(month, '%Y%m')) - relativedelta(months=1)
month = datetime.strftime(date_param, '%Y%m')

level1_quote = level1_quote.withColumn("source_date_start", F.concat(col("source_date"), lit(' 9:30:00')).cast('timestamp')).withColumn("source_date_end", F.concat(col("source_date"), lit(' 16:00:00')).cast('timestamp'))

level1_quote = level1_quote.filter(~(level1_quote.bid_price == 0.0) & ~(level1_quote.bid_price == 0.0) & (level1_quote.ask_price > level1_quote.bid_price) ).withColumn("ask-bid",col("ask_price") - col("bid_price"))

level1_quote = level1_quote.filter(col("source_dtm").between(col('source_date_start'),col('source_date_end')))

QuotedSpread = level1_quote.select("symbol", "ask-bid").groupBy("symbol").agg(count("symbol").alias("quote_count"),avg(col("ask-bid")).alias("quoted_spread")).orderBy('symbol', ascending=True)
QuotedSpread= QuotedSpread.withColumn("order_month",lit(month) ).withColumn("updated",lit('').cast('timestamp'))

QuotedSpread= QuotedSpread.select('symbol','quoted_spread','quote_count','updated','order_month')

QuotedSpread.write.mode("append").format('hive').option("orc.compress","snappy").partitionBy("order_month").saveAsTable("mdw.quoted_spread")