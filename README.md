# IMPACT-OF-SOCIAL-MEDIA-ON-STOCK-MARKET

## Motivation, Background, and Problem Statement

Stock market investment is rewarding but it comes with high risk. There are lots of factors that could
influence stock market such as world event, economy, politics, supply and demand, natural disaster,
etc. The ascension of social media over the past decade has a huge impact on all industries across
sectors but does it affect stock market? The correlation between the web data such as tweets and
the stock market has been studied previously. Based on previous studies there is a significant
dependence between these tweets and stock prices.

## Data Processing Pipeline

### 1. Data Collection:

The Tweepy API is used to import the most recent 3240 tweets from each
user handles. Tweets are then transformed into 2D array which includes tweets and their
relevant dates in a csv format. The Stock data is collected from the NASDAQ website. 

### 2. Data Cleaning:

The tweets were cleaned by applying tokenization and stop words removal
to perform further processing. Entity Extraction is the next applied on these tweets. Named
Entity Recognizer (NER) from Standford, is used to extract company names so the tweets
with recognized company names are selected as the output of this step as shown below.

### 3. Data Fusion and Integration:

As part of data fusion, we combined stock market data
corresponding to company names and date starting from Jan 2016. This data was then
integrated using Spark SQL with the output of sentiment analysis performed on the tweets
obtained from step 2 corresponding to relevant company names and date.

### 4. Data Analysis:

The integrated data containing the following columns "Symbol", "Company",
"Date", "Open", "Close", "High", "Low", "Volume", "Sentiment" is now used to build our
regression model and to analyze agreement of tweets sentiments with stock price
fluctuations.

### 5. Data Product:

Our data product was built using Tableau would help investors with making
short term decisions in investing into the right company based on the current sentiments.

## Methodology

We applied two different methods to analyze the integrated data of tweets and stock prices.

### Our first technique :
was implementing regression on the data to predict the stock prices fluctuations. Our
stock dataset contained the following information for each company relevant to each day: 1- Stock
Symbol, 2- Company Name, 3- Date, 4- Open, 5- Close, 6- High, 7- Low, 8- Volume, 9-Sentiment.

Regression is applied using the above features on 70% of the data set. We kept 20% of the data set
for validation and 10% for test. The accuracy of the analysis is evaluated by checking the difference
between the prediction and actual value of stock fluctuation from each day to the next day. Our
analysis showed that the accuracy is very poor on our test data. One reason behind it could be due
to low number of features that we considered for our model. Based on other studies there are lots of
other factors that could affect the stock market such as GDP, company’s current profits etc., so not
considering these features would lead to poor prediction analysis.

### Our second approach:
was studying the agreement of tweets sentiments with the stock prices
fluctuations. The following parameters were estimated for each person in order to measure the
effectiveness of each person tweets on the stock prices: 1- True Positive, 2- True Negative, 3- False
Positive, 4- False Negative. The model accuracy is measured based on precision, and recall
analysis. This is a great technique that can get advantage of tweets sentiment analysis in order to
study each person tweets effect on stock market. We implemented all our codes in Python Spark,
and used Tableau for visualizations.

## Data Product

Our data product would help investors make short-term investment decisions based on the current
stock prices and sentiments regarding companies in the market. Keeping this vision in mind, we
decided to answer the following questions.

1. How does a sentiment impact an industry as a whole?

2. Which person has the highest impact on the stock market based on their social media
profile?

3. How is the stock market performing before and after the 2016 presidential elections?




## How to Run the Project:
"As the size of our dataset is large, we could not submit the tag final in courses, so we just submitted the python codes and the readme file. However, tag final includes all the files needed to run the project "


### File Descriptions
pystock-data-gh-pages: Original stock input file

tweet_dumper.py: code to download tweets

entity.py: code to extract tweets that includes an organization entity

sentiment.py: code to find the sentiment of each tweet

data_fusion.py: code to combine stock market data corresponding to company and date

read_data.py: create parquet files for stock data and twitter sentimental analysis

analysis.py: combine stock market and twitter data and perform regression

positive_negative_counts.py: Get the True Positive, True Negative, False Positive, False Negative counts for each person of interest

Regression.py: Regression with 3 fold cross validation on the combined to stock and twitter data to predict percentage change

We have provided the below input files for final analysis in parquet format as it's easier to process 
stock.parquet: Processed data in parquet format
twitter_results.parquet: Processed twitter results in parquet format

