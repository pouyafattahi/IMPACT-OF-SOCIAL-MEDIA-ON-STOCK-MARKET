import unirest
import numpy as np
import csv
import json
from pyspark.sql import SQLContext
from aylienapiclient import textapi
#total is the tweets for person of interest detected having an entity of organization
with open('total.csv') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
#new3 is the converted csv file to json 
with open('new3.json', 'w') as f:
    json.dump(rows, f)
sqlCt = SQLContext(sc)
trainDF = sqlCt.read.json('new17.json')
row=trainDF.select('text').collect()
idd=trainDF.select('id').collect()
# These code snippets use an open-source library.
#print row[0]
#x=np.array(["i am a good girl","you are a bad guy"]) test sentences
#print x[1]
a=np.chararray(shape=(len(row),2))
v4=[[0]*2 for i in range(len(row))]
c=[]
for i in range(0,len(row)):
        response = unirest.post("https://community-sentiment.p.mashape.com/text/",
          headers={
            "X-Mashape-Key": "OVqmrla3f6msh1mkgxgH95fQ8zUbp1y8EhyjsnL81oJx8drhgf",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
          },
          params={
            "txt": row[i]
            }
        )
        
        v4[i][0]=(idd[i])
        v4[i][1]= (response.body['result']['sentiment'])

        