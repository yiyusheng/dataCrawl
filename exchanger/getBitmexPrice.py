# -*- codeing:utf-8 -*-
import bitmex,time
import pandas as pd
from sqlalchemy import create_engine 
from datetime import datetime,timedelta

# launch a bitmex client
client = bitmex.bitmex(test=False,api_key='8L8g1w_JwrhgAHNhBpCNYMgH',api_secret='Gx1wdq7ZZXxyIENfhVCwxZpizgbAlan8OC3OIj7ed5WUIfqW')

# connect to mysql
conn = create_engine('mysql+pymysql://root:qwer1234@localhost:3306/prichat?charset=utf8')   

# get data per second and store in to mysql
start_ts = datetime.strptime('2017-01-01 00:00:00','%Y-%m-%d %H:%M:%S')
counter=1
while(1):
    t=client.Trade.Trade_getBucketed(partial=False,binSize='1m',symbol='XBTUSD',count=480,startTime=start_ts).result()
    df=pd.DataFrame(t[0])
    df['timestamp'] = df['timestamp'].apply(lambda x: pd.to_datetime(str(x)))
    df = df.fillna(0)
    df.to_sql('price_bitmex',con=conn,if_exists='append',index=False)
    if(start_ts > datetime.now()):
        break
    print('start:%s\tcounter:%d' %(start_ts,counter))
    start_ts = start_ts + timedelta(minutes=480)
    counter = counter+1
    time.sleep(1)

