import unirest
import numpy as np
import csv
import json
from pyspark.sql import SQLContext
from aylienapiclient import textapi

client = textapi.Client("18a0dbde", "31295bfd17cac9e7a38679d8ac56fce4")
A=[]

#total is the ccsv file of downloaded tweets for each person of interest
with open('total.csv') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
with open('new1.json', 'w') as f:
    json.dump(rows, f)
	
sqlCt = SQLContext(sc)
trainDF = sqlCt.read.json('new1.json')
row=trainDF.select('text').collect()

for i in range (0,len(row)):
    
    entities = client.Entities({"text": row[i]})
   # print (entities['entities'])
    if (entities['entities']=={}) or ('organization' not in entities['entities']):
        if i==len(text):
            break
        
    else:
        for  values in entities['entities']['organization']:
            print entities['entities']
            A.append(values)
        