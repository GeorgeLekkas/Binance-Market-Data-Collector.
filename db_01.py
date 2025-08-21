from binance.client import Client
from datetime import datetime,timedelta
import time
import pandas as pd
from sqlalchemy import create_engine ,text



st=time.time()

client = Client()
engine = create_engine("mysql+pymysql://root:lek@localhost:3306/clp_5m", echo=True)

# =======================================================================================================
fores=3    # 
inte=5   # interval

t0=timedelta(minutes=500*inte)
t1=timedelta(minutes=501*inte)
l_start=[]
l_end=[]

def date_to_milliseconds(dt):
    return int(dt.timestamp() * 1000)

st1=datetime(2025,7,7,14,30)
en1=st1+t0
l_start.append(st1)
l_end.append(en1)
for i in range(1,fores+1):
    l_start.append(l_start[i-1]+t1)
    l_end.append(l_end[i-1]+t1)

ml_st=[date_to_milliseconds(xx) for xx in l_start]    
ml_en=[date_to_milliseconds(xx) for xx in l_end]       # diafora =501 steps.

# ---------------------------pairnv ta tiker pou mporv na tradarw----------------------------------
exchange_info = client.get_exchange_info()
tickers = [s['symbol'] for s in exchange_info['symbols'] if s['status'] == 'TRADING']

# --------------------------------------------------------------------------------------------------

tck = tickers.pop(0)  
datet=[]
prt=[]
for i in range(0,fores+1):
    tms = client.get_klines(symbol=tck, interval=Client.KLINE_INTERVAL_5MINUTE,
                            startTime=ml_st[i], endTime=ml_en[i])
    date =[ datetime.fromtimestamp(candle[0] / 1000)for candle in tms]
    pr=        [ float(candle[4]) for candle in tms]
    datet.extend(date)
    prt.extend(pr)

dfe = pd.DataFrame({'Date': datet, tck: prt})
dfe.set_index('Date', inplace=True)

# -----------------------------------------------------------------------------------------------

nu1=1
nu2=1
for tick in tickers:
    tms = client.get_klines(symbol=tick,interval=Client.KLINE_INTERVAL_5MINUTE,
                               startTime=ml_st[0],endTime=ml_en[0])
    if not tms:
        print(f"Παράλειψη {tick} — χωρίς δεδομένα")
        continue

    prt = [float(candle[4]) for candle in tms]
    for i in range(1, fores + 1):
        tms = client.get_klines(symbol=tck, interval=Client.KLINE_INTERVAL_5MINUTE,
                                startTime=ml_st[i], endTime=ml_en[i])
        pr = [float(candle[4]) for candle in tms]
        prt.extend(pr)


    dfe[tick]=prt
    if nu1==100:
        dfe[f'Indx{nu2}']= range(1, len(dfe) + 1)
        dfe.to_sql(name=f'my_table{nu2}', con=engine, if_exists='replace', index=True)
        print(f'ftiaxthke table :  my_table{nu2}')
        nu2+=1
        nu1=0
        dfe = pd.DataFrame()
    nu1 +=1

if nu1 != 101:
    dfe[f'Indx{nu2}']= range(1, len(dfe) + 1)
    dfe.to_sql(name=f'my_table{nu2}', con=engine, if_exists='replace', index=True)
    print(f'ftiaxthke table :  my_table{nu2} kai teleytaio gia thn omada ')
    nu2+=1
    nu1=0


TT=(time.time()-st)/60
print(f'Total time taken to download all data: {TT} minutes')
st=time.time()
conn = engine.connect()
for nn in range(1,15):
    try:
        query = f""" 
          ALTER TABLE my_table{nn} DROP COLUMN `Index`;
        """
        conn.execute(text(query))
    except:
       print(f'Table {nn} does not have an Index column to remove')


# Ολόκληρο το query
query1 = """

CREATE TABLE final_table1 AS
SELECT
  t1.*, t2.*, t3.*, t4.*, t5.*, t6.*, t7.*
  , t8.*, t9.*
FROM my_table1 t1
JOIN my_table2 t2 ON t1.Indx1 = t2.Indx2
JOIN my_table3 t3 ON t1.Indx1 = t3.Indx3
JOIN my_table4 t4 ON t1.Indx1 = t4.Indx4
JOIN my_table5 t5 ON t1.Indx1 = t5.Indx5
JOIN my_table6 t6 ON t1.Indx1 = t6.Indx6
JOIN my_table7 t7 ON t1.Indx1 = t7.Indx7
JOIN my_table8 t8 ON t1.Indx1 = t8.Indx8
JOIN my_table9 t9 ON t1.Indx1 = t9.Indx9
"""
conn.execute(text(query1))

query2 = """
CREATE TABLE final_table2 AS
SELECT
  t10.*, t11.*,t12.*,t13.*,t14.*,t15.*
FROM my_table10 t10
JOIN my_table11 t11 ON t10.Indx10 = t11.Indx11
JOIN my_table12 t12 ON t10.Indx10 = t12.Indx12
JOIN my_table13 t13 ON t10.Indx10 = t13.Indx13
JOIN my_table14 t14 ON t10.Indx10 = t14.Indx14
JOIN my_table15 t15 ON t10.Indx10 = t15.Indx15
"""
# Εκτέλεση
conn.execute(text(query2))

conn.close()

end=time.time()
xronos=(st-end)/60
print(f'Time taken for merging tables: {xronos} minutes')
print(f'Total time taken to download all data: {TT} minutes')


