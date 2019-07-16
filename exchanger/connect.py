# -*- coding:utf-8 -*-
import requests, pymysql
import pandas as pd

# Launch a bitmex connection
def get_response(url,paras):
    proxy = {'https':'socks5h://127.0.0.1:10800'}
    r = requests.get(url=url,params=paras)
    #r = requests.get(url=url,params=paras,proxies=proxy)
    df = pd.DataFrame(r.json())
    return(df)

def get_conn():
    conn = pymysql.connect(host='127.0.0.1',user='root',passwd='qwer1234',db='prichat',charset='utf8mb4')
    return(conn)
