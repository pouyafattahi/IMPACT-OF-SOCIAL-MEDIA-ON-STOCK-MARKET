from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext
from pyspark.ml.feature import MinMaxScaler, StandardScaler, VectorAssembler, Normalizer
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.mllib.linalg import Vectors, VectorUDT
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from datetime import date


def get_feature_vector(input_df):
	assembler_1 = VectorAssembler(inputCols=["Open", "Close"], outputCol="stock_features")
	scaler = Normalizer(inputCol="stock_features", outputCol="scaled_stock_features")
	assembled_df = assembler_1.transform(input_df)
	scaled_stock = scaler.transform(assembled_df).drop('stock_features')
	assembler_2 = VectorAssembler(inputCols=["scaled_stock_features", "Sentiment"], outputCol="features")
	final_df = assembler_2.transform(scaled_stock)
	return final_df.drop('scaled_stock_features')


conf = SparkConf().setAppName('Linear Regression')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
stock_DF = sqlContext.parquetFile("stocks_full.parquet").registerTempTable('stocks')
twitter_DF = sqlContext.parquetFile("twitter.parquet").registerTempTable('tweets')
lr = LinearRegression(maxIter=10, elasticNetParam=0.8, featuresCol='features')
pipeline = Pipeline(stages=[lr])

paramGrid = (ParamGridBuilder().addGrid(lr.regParam, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])
						   .build())

crossval = CrossValidator(estimator=pipeline,
                      estimatorParamMaps=paramGrid,
                      evaluator=RegressionEvaluator(),
                      numFolds=3)
rmse_evaluator = RegressionEvaluator(metricName="rmse", labelCol="label", predictionCol="prediction")

trump_init_df = sqlContext.sql("""
         SELECT stocks.Symbol, stocks.Company, stocks.Sector, stocks.Industry, stocks.Date, stocks.Open, stocks.Close, stocks.High, stocks.Low, stocks.Volume, stocks.Percentage_Change as label, COALESCE(tweets.Sentiment, 0) as Sentiment
         FROM stocks
         JOIN tweets
         ON stocks.Date = tweets.Date AND stocks.Sector = tweets.Sector AND tweets.Twitter_Handle = 'Trump'
         ORDER BY stocks.Date, stocks.Symbol
     """).dropDuplicates()



	 
trump_init2_df = get_feature_vector(trump_init_df)
trump_df = trump_init2_df.withColumn("label", trump_init2_df["label"].cast(DoubleType()))

split = [.8, .2]    
(trainData, testData) = trump_df.randomSplit(split)


model = crossval.fit(trainData)
trump_prediction = model.transform(testData)
#predictions = model.predict(testData)

trump_rmse = rmse_evaluator.evaluate(trump_prediction)

print trump_prediction.show(truncate=False)
print trump_rmse
trump_prediction.saveAsParquetFile("Trump.parquet")
