#coding: UTF-8 
#########################################################################################
# Developer: Deiner Zapata Silva.
# Date: 20/11/2020
# Last update: 20/11/2020
# Description: Codigo util, para uso general
# sys.setdefaultencoding('utf-8') #reload(sys)
# pip install pycryptodome
#########################################################################################
from urllib.request import urlopen
import pandas as pd
#import eland as ed
#from eland.conftest import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from elastic_app_search import Client
from elasticsearch import Elasticsearch
import re
import time
import datetime
import platform as p
import sys
import multiprocessing as mp
# Import standard test settings for consistent results

# Loading code <utils.py> from github repository.
url_code_github = "https://raw.githubusercontent.com/hoat23/ElasticSearch/master/bin/utils.py"
code_str = urlopen(url_code_github).read()
code_str = code_str.decode('utf-8')
exec(  code_str  )
#########################################################################################
ENDPOINT = "123123.ent-search.us-central1.gcp.cloud.es.io/api/as/v1/"
PRIVATE_KEY = "private-123123"
SEARCH_KEY = "search-123123"
#########################################################################################
if (p.system()=='Windows'):# ONLY WINDOWS
    import winsound 
#########################################################################################
def beep_alert(f = 2500, t = 1000, count = 1):
    if (p.system()=='Windows'):# ONLY WINDOWS
        for i in range(0,count):
            winsound.Beep(f,t)
    return
#########################################################################################
def app_search(ENDPOINT,PRIVATE_KEY):
    client = Client(
        base_endpoint=ENDPOINT,
        api_key=PRIVATE_KEY,
        use_https=True
    )
    rpt = client.list_engines(current=1, size=20)
    print("\nClient list engines")
    print_json(rpt)
    rpt = client.get_search_settings(engine_name='sunat-ruc')
    print("\nEngine Name:")
    print_json(rpt)
    return client
#########################################################################################
def execute_multiprocess(elk,block_data,header_json,list_process=[],num_process=5,cont=0):
    if (num_process<=len(list_process)):
        p = mp.Process(name='process_num_'+str(cont), target=execute_proccess,args=(elk, block_data, header_json) )
        p.daemon = True
        p.start()
        list_process.append(p)
    else:
        for p in list_process:
            p.join()
        p = mp.Process(name='process_num_'+str(cont), target=execute_proccess,args=(elk, block_data, header_json) )
        p.daemon = True
        p.start()
        list_process.append(p)
    return list_process
#########################################################################################
def loadHeavyText2ELK(nameHeavyFile, char_sep = '|',block_size = 1E2, index_name="sunat-ruc",send_elk=False, coding="ISO-8859-1"):
    cont = 0
    input_file = open(nameHeavyFile,'rb')
    print("[{0} | START ]".format(datetime.datetime.utcnow().isoformat() ) )
    block_data=[]
    #elk = elasticsearch()
    elk = app_search(ENDPOINT,PRIVATE_KEY)
    sum_chars = critical_err = 0
    header_line = ""
    while(1):
        cont = cont + 1 
        try:
            line = input_file.readline()
            org_line = line = line.decode(coding)#,errors='replace')
            sum_chars =  sum_chars + len(line)
            line = line.replace("\n","")
            line = line.replace("\r","")
            #line = line.replace("\\||","-|")
            line = line.replace("\\","-")
            line = line.replace("||","|")
            
            re.sub('[^a-zA-Z0-9-_*.]', '', line) #https://platzi.com/blog/expresiones-regulares-python/
            
            if(cont==1):
                header_line = line
                header_fields = line.split(char_sep)
                print(header_fields)
            else:
                data_json = {}
                body_fields = line.split(char_sep)
                data_json, err = list2json(header_fields, body_fields,return_err=True)    
                if(err!=0):
                    print("[{0} | ERROR | num line : {1: 10d} | num byte : {2: 10d}]".format(datetime.datetime.utcnow().isoformat(), cont, sum_chars) )
                    print(" ->  | {0}".format(header_line))
                    print(" ->  | {0}".format(org_line))
                    print(" ->  | {0}".format(line))

                if(len(data_json)==0):
                    print("[{0} | ERROR |* data lost * | num line : {1: 10d} | num byte : {2: 10d}]".format(datetime.datetime.utcnow().isoformat(), cont, sum_chars) )
                    print(" ->  | {0}".format(header_line))
                    print(" ->  | {0}".format(org_line))
                    print(" ->  | {0}".format(line))
                    beep_alert()
                else:
                    data_json.update({'rename_index': index_name,"id":cont})
                    #print_json(data_json)
                    block_data.append(data_json)
                
                if(len(body_fields)==0):
                    critical_err = critical_err + 1
                    print("[{0} | CRIT  | num line : {1: 10d} | num byte : {2: 10d}]".format(datetime.datetime.utcnow().isoformat(), critical_err) )
                
            if(cont%block_size==0):
                print("[{0} | INFO  | num line : {1: 10d}]".format(datetime.datetime.utcnow().isoformat(), cont) )
                if(send_elk and len(block_data)>0):
                    header_json={"index":{"_index": index_name,"_type":"_doc","_id":"id"}}
                    #list_process = execute_multiprocess(elk,block_data,header_json, list_process=list_process)
                    #elk.post_bulk(block_data,header_json=headerjson)
                    engine_name = index_name
                    rpt = elk.index_documents(engine_name,block_data)
                    #print_json(rpt)
                block_data=[]
                #time.sleep(1)
        except:
            print("[{0} | ERROR | {1} ]".format(datetime.datetime.utcnow().isoformat(), cont) )
            if(len(header_line)>0):
                print(" ->  | {0}".format(header_line))
            print(" ->  | {0}".format(org_line))
            print(" ->  | {0}".format(line))
            beep_alert(t=500,count=3)
            critical_err = critical_err + 1
            #break
        finally:
            if(critical_err+1>5): #just support 5 errors
                print("[{0} | CRIT  | {1} ]".format(datetime.datetime.utcnow().isoformat(), critical_err) )
                beep_alert(t=500,count=3)
                #Send last block
                if(send_elk and len(block_data)>0):
                        header_json={"index":{"_index": index_name,"_type":"_doc","_id":"id"}}
                        #list_process = execute_multiprocess(elk,block_data,header_json, list_process=list_process)
                        #elk.post_bulk(block_data,header_json=headerjson)
                        engine_name = index_name
                        rpt = elk.index_documents(engine_name,block_data)
                        print_json(rpt)
                break
    print("[{0} | END   ]".format(datetime.datetime.utcnow().isoformat()))
#########################################################################################
if __name__ == "__main__":
    print("Default encoding: {0}".format(sys.getdefaultencoding()))
    if (p.system()=='Windows'):# ONLY WINDOWS
        full_path='padron_reducido_ruc_2020.txt'
    else:
        full_path='/usr/share/logstash/padron_reducido_ruc_2020.txt'
    loadHeavyText2ELK(full_path, send_elk=True)
