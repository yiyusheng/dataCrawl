# -*- coding:utf-8 -*-
import requests, time, pymysql
import pandas as pd
from datetime import datetime,timedelta

# Launch a bitmex connection
def get_response(url,paras,proxies):
    r = requests.get(url=url,params=paras,proxies=proxy)
    df = pd.DataFrame(r.json())
    return(df)

# Connect to mysql
conn = pymysql.connect(host='127.0.0.1',user='root',passwd='qwer1234',db='prichat',charset='utf8mb4')
cur = conn.cursor()
cur.execute("SELECT max(content_id) FROM chat_logs where group_name='bitmex'")
latest_start = cur.fetchall()[0][0]

# Parameters
proxy = {'https':'socks5h://127.0.0.1:10800'}
url = "https://www.bitmex.com/api/v1/chat"
paras = {
        'count':500,
        'channelID':2,
        'start':latest_start+1,
        'test':False}
        #'start':304825
#url = "https://www.bitmex.com/api/v1/chat?count=500&channelID=2&start=304825&test=False"

while(1):
    df = get_response(url,paras,proxy)
    if(len(df)==0):
        break
    df['time'] = pd.to_datetime(df['date'])
    #df['time'] = df.time.dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
    df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['group_name'] = 'Bitmex'
    df['group_type'] = 'Exchanger'
    df = df.fillna(0)
    
    cur.executemany('INSERT IGNORE INTO chat_logs(time,content,group_name,user,content_id,group_type) VALUES(%s,%s,%s,%s,%s,%s)',df[['time','message','group_name','user','id','group_type']].values.tolist())
    conn.commit()

    paras['start'] = max(df['id'])+1
    print('start_time_utc:%s\tcounter:%d\ttime_local:%s' %(df['time'][0],paras['start'],datetime.now()))
    time.sleep(2)
 
cur.close()
conn.close()
            
