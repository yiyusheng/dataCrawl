# -*- codeing:utf-8 -*-
import bitmex,time,pymysql,time
import pandas as pd
from datetime import datetime,timedelta

# launch a bitmex client
client = bitmex.bitmex(test=False,api_key='8L8g1w_JwrhgAHNhBpCNYMgH',api_secret='Gx1wdq7ZZXxyIENfhVCwxZpizgbAlan8OC3OIj7ed5WUIfqW')

# connect to mysql
conn = pymysql.connect(host='127.0.0.1',user='root',passwd='qwer1234',db='prichat',charset='utf8mb4')
cur = conn.cursor()

# get data per second and store in to mysql
start_id = 1
while(1):
    t=client.Chat.Chat_get(count=500,start=start_id,channelID=2).result()
    df=pd.DataFrame(t[0])
    df['create_time'] = df['date'].apply(lambda x: pd.to_datetime(str(x)))
    df['create_time'] = df['create_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df = df.fillna(0)
    df['group_name'] = 'Bitmex'
    
    cur.executemany('INSERT IGNORE INTO chat_logs(create_time,content,group_name,nickname,mark) VALUES(%s,%s,%s,%s,%s)',df[['create_time','message','group_name','user','id']].values.tolist())
    conn.commit()
    start_id = max(df['id'])+1
    print('start:%s\tcounter:%d' %(df['create_time'][0],start_id))
    time.sleep(1)

cur.close()
conn.close()
