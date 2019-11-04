# coding: utf-8
# Developer: Deiner Zapata Silva.
# Date: 24/10/2019
# Last Update: 04/11/2019
# Description: Download some tables of a web page
# Documentation and other sources codes
# https://srome.github.io/Parsing-HTML-Tables-in-Python-with-BeautifulSoup-and-pandas/
#########################################################################################
import requests
import lxml.html as lh
import pandas as pd 
from bs4 import BeautifulSoup as bs 
#########################################################################################
URL = "https://docs.microsoft.com/es-us/office365/enterprise/urls-and-ip-address-ranges?redirectSourcePath=%252farticle%252fOffice-365-URLs-and-IP-address-ranges-8548a211-3fe7-47cb-abb1-355ea5aa88a2"
#########################################################################################
def parse_html_table(table):
    """
    Read every value in a table, every columns and rows
    and return table a dataframe format
    """
    n_columns = 0
    n_rows=0
    column_names = []

    # Find number of rows and columns
    # we also find the column titles if we can
    for row in table.find_all('tr'):
        
        # Determine the number of rows in the table
        td_tags = row.find_all('td')
        if len(td_tags) > 0:
            n_rows+=1
            if n_columns == 0:
                # Set the number of columns for our table
                n_columns = len(td_tags)
                
        # Handle column names if we find them
        th_tags = row.find_all('th') 
        if len(th_tags) > 0 and len(column_names) == 0:
            for th in th_tags:
                column_names.append(th.get_text())

    # Safeguard on Column Titles
    if len(column_names) > 0 and len(column_names) != n_columns:
        raise Exception("Column titles do not match the number of columns")

    columns = column_names if len(column_names) > 0 else range(0,n_columns)
    df = pd.DataFrame(columns = columns, index= range(0,n_rows))
    row_marker = 0
    for row in table.find_all('tr'):
        column_marker = 0
        columns = row.find_all('td')
        for column in columns:
            df.iat[row_marker,column_marker] = column.get_text()
            column_marker += 1
        if len(columns) > 0:
            row_marker += 1
            
    # Convert to float if possible
    for col in df:
        try:
            df[col] = df[col].astype(float)
        except ValueError:
            pass

    return df
#########################################################################################
if False:
    ####################### DOWNLOADING WEB PAGE 
    rpt = requests.get(URL)
    content_html = rpt.content
    doc = lh.fromstring(content_html)
    tr_elements = doc.xpath('//tr')#parser data that are atored between <tr>..</tr> of HTML
    soup = bs(content_html, 'lxml') #'html.parser' 'lxml'
    #print(soup.prettify)
    ####################### LISTING TABLES IN WEB PAGE 
    tables = soup.find_all('table')[1:]
    df_list = []
    for table in tables:
        df_new = parse_html_table(table)
        print(df_new.shape)
        #print(df_new.columns.values)
        df_filter = df_new[df_new.columns[3:]]
        df_list.append(df_filter)
    # Group all in a only dataframe
    df_all = pd.concat(df_list , axis=0)
    #print ( df_all )
    dict_clasification = {
        "03100": "ipv4",
        "30100": "ipv4",
        "10001": "url",
        "20001": "url",
        "20002": "url",
        "30001": "url",
        "30002": "url",
        "40001": "url",
        "40002": "url",
        "50002": "url",
        "04100": "ipv6",
        "05100": "ipv6",
        "06100": "ipv6",
        "50011": "wc_url",
        "30011": "wc_url",
        "30012": "wc_url",
        "40011": "wc_url",
        "20011": "wc_url",
        "20012": "wc_url",
        "20021": "wc_grp"
    }
    # Working with every row
    elements_list = []
    f = open("webfilter.csv", "w+")
    for index, row in df_all.iterrows():
        temp_list = row.tolist()
        # Working with urls and ips
        string =temp_list[0].replace(" ","")
        list_ips_urls = string.split(",")
        # working with port
        string = temp_list[1].replace(" ","")
        string = string.replace("TCP:","")
        list_ports  = string.split(",")
        #print(list_ips_urls)
        for element in list_ips_urls:
            #print("-----------------------------------------")
            attr1 = element.count(".")
            attr2 = element.count(":")
            attr3 = element.count("/")
            attr4 = element.count("*") + element.count("<") +  element.count(">")
            attr5 = element.count(".com") + element.count(".net") + element.count(".org") + element.count( ".ms" ) + element.count( ".by" )
            array_caract = [attr1, attr2, attr3, attr4, attr5]
            str_caract= "{0}{1}{2}{3}{4}".format(attr1, attr2, attr3, attr4, attr5)
            try:
                label = dict_clasification[str_caract]
            except:
                print("ERRROR | {1} | {0}".format(str_caract , element))
                label = "error"
            #print("{3}\t| {0} | {1}\t | {2}".format (str_caract, label,  element, list_ports) )
            for port in list_ports:
                string_data = "{3},{0},{1},{2}".format (str_caract, label,  element, port)
                f.write(string_data+"\n")
    f.close()
################################################################################################################ 


