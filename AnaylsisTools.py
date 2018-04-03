#CSV Writer

import csv
import tqdm
import pymongo
import pandas as pd


DB_NAME = "Ethereum_Blockchain"
COLLECTION = "Transactions_1"

def blockparser(doclist):
    allrows = []
    for doc in doclist:
        if block["transactions"]:
            for transaction in block["transactions"]:
                data = [block["number"], block["timestamp"], transaction['from'], transaction['to'],
                       transaction['value'], transaction['data']]
                allrows.append(data)
        else:
            data = [block["number"], block["timestamp"]]
            allrows.append(data)
    return allrows

def csvdump(filename, doclist):
    allrows = blockparser(doclist)
    with open(filename, "w") as file:
        csvwriter = csv.writer(file, delimiter=",")
        csvwriter.writerows(allrows)
    print("Done!")

    #Slice mongo db by dates


import pymongo
from datetime import datetime

DB_NAME = "Ethereum_Blockchain"
COLLECTION = "Transactions_1"

def unixtostringtime(time):
    return datetime.fromtimestamp(
        int(time)
    ).strftime('%Y-%m-%d %H:%M:%S')

def stringtounixtime(time):
    #Time format: %Y-%m-%d %H:%M:%S
    return str(int(datetime.strptime(time,'%Y-%m-%d %H:%M:%S').timestamp()))

def MongoSlice(startdate, enddate):
    """Time format is: %Y-%m-%d %H:%M:%S"""
    client = pymongo.MongoClient()
    db = client[DB_NAME][COLLECTION]
    unixstartdate = stringtounixtime(startdate)
    unixenddate = stringtounixtime(enddate)
    doclist = db.find({"timestamp": {"$gt": unixstartdate,"$lt": unixenddate}})
    return doclist

def MongoSliceandProcess(startdate, enddate):
    doclist = MongoSlice(startdate, enddate)
    docprocessed = blockparser(doclist)
    
    headers = ['blocknumber','timestamp','from','to','value','data']
    frame = pd.DataFrame(docprocessed, columns = headers)
    return frame


-------------------------------
#Pandas script
%matplotlib inline

import matplotlib
import matplotlib.pyplot as plt
import os
import pandas as pd

def transactioncount(frame, counttokens = False):
    if not counttokens:
        frame = frame[frame['value'] != 0]
    fromlistcounts = frame['from'].value_counts().reset_index()
    tolistcounts = frame['to'].value_counts().reset_index()
    combined = pd.merge(fromlistcounts, tolistcounts, on=['index'], how = 'outer', suffixes = ['_x','_y']).fillna(0)
    combined['sum'] = combined['0_x'] + combined['0_y']
    return combined

def topPercentile(frame, percentile):
    #percentile as whole number (e.g. 95 = 95th percentile)
    newframe = frame[frame['sum'] < np.percentile(frame['sum'],percentile)]
    return newframe




   


