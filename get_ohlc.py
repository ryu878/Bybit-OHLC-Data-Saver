# Ryuryu's Bybit OHLC Data Saver
# SQLite Edition
# ---------------------------------
# (c) 2023 Ryan Hayabusa 
# Github: https://github.com/ryu878 
# Web: https://aadresearch.xyz
# Discord: ryuryu#4087
# ---------------------------------

import json 
import sqlite3
import requests 
import pandas as pd
import datetime as dt
from time import sleep
from inspect import currentframe


### Settings Block Start ###

symbol = 'ETHUSDT'

year = 2023
month = 1
day = 1

### Settings Block End   ###


def get_linenumber():
    cf = currentframe()
    global line_number
    line_number = cf.f_back.f_lineno


def get_bybit_bars(symbol, interval, startTime, endTime):

    url = 'https://api.bybit.com/public/linear/kline'
    startTime = str(int(startTime.timestamp()))
    endTime   = str(int(endTime.timestamp()))
    req_params = {'symbol' : symbol, 'interval' : interval, 'from' : startTime, 'to' : endTime}
    df = pd.DataFrame(json.loads(requests.get(url, params = req_params).text)['result'])
    if (len(df.index) == 0):
        return None
    
    df.index = [dt.datetime.fromtimestamp(x) for x in df.open_time]
    return df


df_list = []
last_datetime = dt.datetime(year, month, day)

while True:

    print(last_datetime)
    new_df = get_bybit_bars(symbol, 1, last_datetime, dt.datetime.now())
    if new_df is None:
        break
    df_list.append(new_df)
    last_datetime = max(new_df.index) + dt.timedelta(0, 1)
    df = pd.concat(df_list)

    print(df)

    try:               

        conn = sqlite3.connect('ohcl.db')
        df.to_sql(f'{symbol}', conn, if_exists='replace', index=False)

        conn.commit()
        conn.close()
            
    except Exception as e:
        get_linenumber()
        print(line_number, 'exeception: {}'.format(e))


    sleep(0.1)
