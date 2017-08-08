from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf

conf = SparkConf().setAppName('Analysis')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
stock_DF = sqlContext.parquetFile("stocks.parquet").registerTempTable('stocks')
twitter_DF = sqlContext.parquetFile("twitter_results.parquet").registerTempTable('tweets')

trump_df = sqlContext.sql("""
         SELECT stocks.Symbol, stocks.Company, stocks.Sector, stocks.Date, stocks.Open, stocks.Close, stocks.High, stocks.Low, stocks.Volume, stocks.Percentage_Change, COALESCE(tweets.Sentiment, 0) as Sentiment
         FROM stocks
         JOIN tweets
         ON stocks.Date = tweets.Date AND stocks.Sector = tweets.Sector AND tweets.Twitter_Handle = 'Trump'
         ORDER BY stocks.Date, stocks.Symbol
     """).dropDuplicates()

hillary_df = sqlContext.sql("""
         SELECT stocks.Symbol, stocks.Company, stocks.Sector, stocks.Date, stocks.Open, stocks.Close, stocks.High, stocks.Low, stocks.Volume, stocks.Percentage_Change, COALESCE(tweets.Sentiment, 0) as Sentiment
         FROM stocks
         JOIN tweets
         ON stocks.Date = tweets.Date AND stocks.Sector = tweets.Sector AND tweets.Twitter_Handle = 'Hillary'
         ORDER BY stocks.Date, stocks.Symbol
     """).dropDuplicates()

elon_df = sqlContext.sql("""
         SELECT stocks.Symbol, stocks.Company, stocks.Sector, stocks.Date, stocks.Open, stocks.Close, stocks.High, stocks.Low, stocks.Volume, stocks.Percentage_Change, COALESCE(tweets.Sentiment, 0) as Sentiment
         FROM stocks
         JOIN tweets
         ON stocks.Date = tweets.Date AND stocks.Sector = tweets.Sector AND tweets.Twitter_Handle = 'Elon'
         ORDER BY stocks.Date, stocks.Symbol
     """).dropDuplicates()

WH_df = sqlContext.sql("""
         SELECT stocks.Symbol, stocks.Company, stocks.Sector, stocks.Date, stocks.Open, stocks.Close, stocks.High, stocks.Low, stocks.Volume, stocks.Percentage_Change, COALESCE(tweets.Sentiment, 0) as Sentiment
         FROM stocks
         JOIN tweets
         ON stocks.Date = tweets.Date AND stocks.Sector = tweets.Sector AND tweets.Twitter_Handle = 'WhiteHouse'
         ORDER BY stocks.Date, stocks.Symbol
     """).dropDuplicates()

def get_count(init_df):
    def effect_count(change, sentiment):
        if sentiment == 1 and change > 0.0:
            effect = 'true positive'
        elif sentiment == 1 and change < 0.0:
            effect = 'true negative'
        elif sentiment == -1 and change < 0.0:
            effect  = 'false negative'
        elif sentiment == -1 and change > 0.0:
            effect = 'false positive'
        else:
            effect = 'null'
        return effect

    udf_effect = udf(effect_count,StringType())
    stock_effect = init_df.withColumn('Effect', udf_effect(init_df.Percentage_Change, init_df.Sentiment)).cache()
    TP_count = stock_effect.filter(stock_effect.Effect == 'true positive').count()
    TN_count = stock_effect.filter(stock_effect.Effect == 'true negative').count()
    FP_count = stock_effect.filter(stock_effect.Effect == 'false positive').count()
    FN_count = stock_effect.filter(stock_effect.Effect == 'false negative').count()
     
    return [TP_count, TN_count, FP_count, FN_count]

trump_count = get_count(trump_df)
hillary_count = get_count(hillary_df)
elon_count = get_count(elon_df)
WH_count = get_count(WH_df)

print "Person = True Positive, True Negative, False Positive, False Negative"
print "Trump = ", trump_count
print "Hillary = ", hillary_count
print "Elon =", elon_count
print "The WhiteHouse = ", WH_count
