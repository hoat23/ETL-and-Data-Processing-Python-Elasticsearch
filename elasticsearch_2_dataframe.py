#coding: UTF-8 
#########################################################################################
# Developer: Deiner Zapata Silva.
# Date: 19/11/2018
# Last update: 23/07/2019
# Description: Procesing data of elasticsearch with pandas
# Notes: Elastic only support binary data encoded in base64.
# sys.setdefaultencoding('utf-8') #reload(sys)
# {'index.routing.allocation.include.instance_configuration':'aws.data.highstorage.d2'}
#########################################################################################
import sys
import requests
import json
import time
import pprint
import pandas as pd
from datetime import datetime, timedelta
from credentials import * #URL="<elastic>" #USER="usr_elk"  #PASS="pass_elk"
from utils import *
from elasticsearch import Elasticsearch
from pandas import DataFrame, Series
#######################################################################################
# 0. Connecting to Elasticsearch cluster
es = Elasticsearch([URL], http_auth=(USER, PASS) )
total_docs = 10

# 1. Getting docs from elasticsearch
res = es.search(
    index="repsol*",
    body={
        "query": {
            "bool": {
                "must": [
                    {"term": { "mes": "Oct-19"}},
                    {"term": { "tipo_reporte": "vulnerabilidades"}}
                ]
            }
        },
#        "_source": ['tipo_producto','hostname','sistema_operativo']
    },
    size=total_docs,
    )
#print_json(elastic_docs)

# 2. Filtering only docs
# http://www.diegocalvo.es/dataframes-in-python/
_source = getelementfromjson(res,"hits.[hits]._source")
table = DataFrame(_source)
filter_columns = [
    'ip_address',
    'dns_name',
    'NetBIOS Name'
]
#table.drop( filter_columns , axis=1, inplace=True)
headers = table.columns.values
print("\nList of columns names . . .")
print_list(headers)
print("\nData filtered . . .")
df = table[ filter_columns  ]
#print(table.head())
print(df.head())

# 3. Group by ip
print("\nGroup by IP . . .")
g_ip = df.groupby('ip_address').count()
print(g_ip)

# 4. Delete 
print("\nDeleted duplicate . . .")
df = df['ip_address'].drop_duplicates()
print(df)