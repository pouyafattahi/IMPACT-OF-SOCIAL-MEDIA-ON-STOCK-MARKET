from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, DateType
from pyspark.sql.functions import udf

conf = SparkConf().setAppName('Read Stock Market Data')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
prices = "prices_final.csv"
symbols = "symbols_final.txt"
sectors = "sectors_final.csv"
twitter_sentiments = "twitter_results.csv"

def prices_schema():
    schema = StructType([
            StructField('Symbol', StringType(), False),
            StructField('Date', StringType(), False),
            StructField('Open', FloatType(), False),
            StructField('High', FloatType(), False),
            StructField('Low', FloatType(), False),
            StructField('Close', FloatType(), False),
            StructField('Volume', FloatType(), False),
            StructField('Adjusted_Close', FloatType(), False)
    ])
    return schema

def sentiment_schema():
    schema = StructType([
            StructField('Twitter_Handle', StringType(), False),
            StructField('ID', IntegerType(), False),
            StructField('Symbol', StringType(), False),
            StructField('Company', StringType(), False),
            StructField('Date', DateType(), False),
            StructField('Sentiment', IntegerType(), False),
            StructField('Sector', StringType(), False),
            StructField('Industry', StringType(), False)
    ])
    return schema

def sectors_schema():
    schema = StructType([
            StructField('Symbol', StringType(), False),
            StructField('Name', StringType(), False),
            StructField('Last_Scale', StringType(), False),
            StructField('Market_Cap', StringType(), False),
            StructField('ADR_TSO', StringType(), False),
            StructField('IPO_year', StringType(), False),
            StructField('Sector', StringType(), False),
            StructField('Industry', StringType(), False),
            StructField('Summary_Quote', StringType(), False),
    ])
    return schema

def create_row(line):
    row_field_name = Row("Symbol","Company")
    row_log = row_field_name(line[0], line[1])
    return row_log

def get_stock_dataframe():
	prices_DF = sqlContext.read.format('com.databricks.spark.csv')\
						.schema(prices_schema())\
						.options(header='true')\
						.load(prices)\
						.dropDuplicates().cache()


	text = sc.textFile(symbols).map(lambda l: l.split('\t'))
	symbols_dict = text.collectAsMap()
	for k,v in symbols_dict.iteritems():
		symbols_dict[k] = v.replace("&#39;","\'")

	def integrate_company(symbol_prices):
		for key, value in symbols_dict.iteritems():
			if key == symbol_prices:
				company = value
		return company


	udf_company = udf(integrate_company, StringType())
	company_DF = prices_DF.withColumn('Company', udf_company(prices_DF.Symbol)).cache()

	sectors_DF = sqlContext.read.format('com.databricks.spark.csv')\
						.schema(sectors_schema())\
						.options(header='true')\
						.load(sectors)\
						.dropDuplicates().cache()

	sectors_dict = map(lambda row: row.asDict(), sectors_DF.collect())

	def integrate_sector(symbol_prices):
		sector_value = ""
		for item in sectors_dict:
			if item['Symbol'] == symbol_prices:
				sector_value = item['Sector']
		return sector_value

	udf_sector = udf(integrate_sector, StringType())
	stock_sec_DF = company_DF.withColumn('Sector', udf_sector(company_DF.Symbol))

	def integrate_industry(symbol_prices):
		industry_value = ""
		for item in sectors_dict:
			if item['Symbol'] == symbol_prices:
				industry_value = item['Industry']
		return industry_value

	udf_industry = udf(integrate_industry, StringType())
	stock_DF = stock_sec_DF.withColumn('Industry', udf_industry(stock_sec_DF.Symbol))

	stock_new_DF = stock_DF.withColumn("Date", stock_DF["Date"].cast(DateType())).orderBy("Date", "Symbol")

	def percent_change(open, close):
		change = (open-close)/open
		return change

	udf_percentchange = udf(percent_change, FloatType())
	stock_final_DF = stock_new_DF.withColumn('Percentage_Change', udf_percentchange(stock_new_DF.Open, stock_new_DF.Close))

	stock_final_DF.saveAsParquetFile("stocks_full.parquet")
	
	return stock_final_DF

def get_twitter_dataframe():
	twitter_DF = sqlContext.read.format('com.databricks.spark.csv')\
						.schema(sentiment_schema())\
						.options(header='true')\
						.load(twitter_sentiments)\
						.dropDuplicates().orderBy("Date", "Symbol").cache()
	twitter_DF.saveAsParquetFile("twitter.parquet")
	return twitter_DF

stock_df = get_stock_dataframe()
twitter_df = get_twitter_dataframe()

