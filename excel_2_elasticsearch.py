#!/usr/bin/env python
#coding: utf-8
#########################################################################################
# Developer: Deiner Zapata Silva.
# Date: 06/03/2020
# Last Update: 06/03/2020
# Description: Codigo para procescesar datos de REPSOL 
# sys.setdefaultencoding('utf-8') #reload(sys)
#######################################################################################
import requests
import yaml #pyyaml
import json
import sys
import time
import datetime
import os
import pandas as pd
from pandas import read_excel
from pandas.io.json import json_normalize
from utils import save_yml, print_json, loadCSVtoJSON, renameKeys, send_json
from elastic import *
#######################################################################################
def process_buckets_documents(bucket_documents, index_name):
    elk = elasticsearch()
    #Sending Bucket to Elasticsearch
    rpt = elk.post_bulk(bucket_documents,header_json={"index":{"_index":index_name ,"_type":"_doc"}})
    try:
        if 'errors' in rpt:
            if rpt['errors'] :
                print(" INFO  | process_buckets_documents | Response from ElasticSearch | Errors: {0}".format( rpt['errors'] ))
        else:
            print(" ERROR | process_buckets_documents | Response from ElasticSearch | error_field_not_found")
            print_json(rpt)
            input(" INFO  | process_buckets_documents | error_field_not_found | Press any key to continue ...")
    finally:
        input(" INFO  | process_buckets_documents | Data loaded into index={0}".format( index_name ) )
    except:
        print(" ERROR | process_buckets_documents | Catch a except | Response from ElasticSearch")
        print_json(rpt)
        input(" INFO  | process_buckets_documents | Catch a except | Press any key to continue ...")
#######################################################################################
def xlsx_2_pandas(fullpath, name_sheet, drop_colums=[], drop_rows=[], delete_columns_name=False): 
    only_name_file = os.path.basename(fullpath)
    fullpath_name, extension = os.path.splitext(fullpath)
    df = None
    if extension == '.xlsx':
        df = read_excel(fullpath, sheet_name = name_sheet) #Global, PERU
        #if delete_columns_name:
        del df.columns.name
        print("extract_sheet_from_xlsx | {1} | {0}".format(only_name_file, df.shape))
        #print(df.head())
    else:
        print("xlsx_2_pandas | ERR | {0}".format(only_name_file))
    return df
#######################################################################################
if __name__ == "__main__":
    #Reading File
    relative_path = ".\\data\\CMDB\\"
    nameFile =  relative_path + "Inventario de Equipos CLIENTE.xlsx"
    df = xlsx_2_pandas(nameFile, "switches", delete_columns_name=True)
    
    # gettig type of data and deleting a row
    df = df.drop(['id'], axis=1)
    df = df.drop([0], axis=0)
    key_fields_ecs = df.iloc[0,:] # 0:Primera fila   1: Segunda fila    -1: Ultima fila

    # Selecting some fields
    #fullobserver -> df_observer = df[["observer.geo.building","observer.geo.city_name","observer.geo.continent_name","observer.geo.country_iso_code","observer.geo.country_name","observer.geo.floor","observer.geo.location","observer.geo.name","observer.geo.rack","observer.geo.rack_unit","observer.geo.region_iso_code","observer.geo.region_name","observer.geo.room","observer.geo.site","observer.hostname","observer.ip","observer.mac","observer.name","observer.os.family","observer.os.full","observer.os.kernel","observer.os.name","observer.os.platform","observer.os.version","observer.product","observer.serial_number","observer.type","observer.vendor","observer.version"]]
    df_observer = df[["observer.geo.building","observer.geo.city_name","observer.geo.continent_name","observer.geo.country_iso_code","observer.geo.country_name","observer.geo.floor","observer.geo.name","observer.geo.rack","observer.geo.rack_unit","observer.geo.region_iso_code","observer.geo.region_name","observer.geo.room","observer.geo.site","observer.hostname","observer.ip","observer.mac","observer.name","observer.os.family","observer.os.full","observer.os.kernel","observer.os.name","observer.os.platform","observer.os.version","observer.product","observer.serial_number","observer.type","observer.vendor","observer.version"]]
    observer_rename = {"observer.geo.building": "geo.building", "observer.geo.city_name": "geo.city_name", "observer.geo.continent_name": "geo.continent_name", "observer.geo.country_iso_code": "geo.country_iso_code", "observer.geo.country_name": "geo.country_name", "observer.geo.floor": "geo.floor", "observer.geo.location": "geo.location", "observer.geo.name": "geo.name", "observer.geo.rack": "geo.rack", "observer.geo.rack_unit": "geo.rack_unit", "observer.geo.region_iso_code": "geo.region_iso_code", "observer.geo.region_name": "geo.region_name", "observer.geo.room": "geo.room", "observer.geo.site": "geo.site", "observer.hostname": "hostname", "observer.ip": "ip", "observer.mac": "mac", "observer.name": "name", "observer.os.family": "os.family", "observer.os.full": "os.full", "observer.os.kernel": "os.kernel", "observer.os.name": "os.name", "observer.os.platform": "os.platform", "observer.os.version": "os.version", "observer.product": "product", "observer.serial_number": "serial_number", "observer.type": "type", "observer.vendor": "vendor", "observer.version": "version"}
    df_observer = df_observer.rename( columns = observer_rename )
    
    # Converting to json
    df_observer_str = df_observer.to_json(orient= 'records') # orient : records, index, values, table, columns(default)
    bucket_documents = json.loads(df_observer_str)
    process_buckets_documents(bucket_documents, "ecs-devices_enrichment_by_ip-switch-senati")
    pass
