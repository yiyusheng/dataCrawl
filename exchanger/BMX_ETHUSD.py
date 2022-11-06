# -*- coding:utf-8 -*-
import requests, time, pymysql
import pandas as pd
from datetime import datetime,timedelta
from connect import get_response,get_conn

if __name__ == '__main__':
# Connect to mysql
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT max(timestamp) FROM bitmex_price where symbol='ETHUSD'")
    latest_ts = cur.fetchall()[0][0]

# Parameters
    url = "https://www.bitmex.com/api/v1/trade/bucketed"
    paras = {
            'test':False,
            'partial':False,
            'binSize':'1m',
            'symbol':'ETHUSD',
            'count':480,
            'startTime':latest_ts+timedelta(minutes=1),
            #'startTime':datetime.strptime('2018-08-01 00:00:00','%Y-%m-%d %H:%M:%S')
            }

    while(1):
        df = get_response(url,paras)
        if len(df)==0:
            break
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['homeNotional'] = df['homeNotional'].round(5)
        df = df.fillna(0)

        cur.executemany('INSERT IGNORE INTO bitmex_price(close,foreignNotional,high,homeNotional,lastSize,low,open,symbol,timestamp,trades,turnover,volume,vwap) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',df.values.tolist())
        conn.commit()

        print('start_time_utc:%s\ttime_local:%s' %(paras['startTime'],datetime.now()))
        paras['startTime'] = paras['startTime']+timedelta(minutes=480)
        time.sleep(2)
     
    cur.close()
    conn.close()
                
