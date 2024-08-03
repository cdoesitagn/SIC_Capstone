from confluent_kafka import Producer
import requests
import json
import time
import datetime
import logging
import src.data as dt


# Kafka Producer Configuration
log_filename = f"logs/{datetime.datetime.now().strftime('%Y-%m-%d')}_producerRT_script.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
producer = Producer({'bootstrap.servers': 'localhost:9092'})
tickers = ['BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 'MBB', 'MSN', 'MWG', 'PLX', 'POW', 'SAB', 'SHB', 'SSB', 'SSI', 'STB', 'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE']
open_time = datetime.time(9, 15)
close_time = datetime.time(15, 0)
day_date = datetime.datetime.today().date()
# day_date_formated = day_date.strftime('%Y-%m-%d')
day_date_formated = "2024-08-02"
loader = dt.DataLoader_json(symbols=tickers,
           start=day_date_formated,
           end=day_date_formated,
           minimal=False,
           data_source="cafe",
           table_style="prefix")
# while (datetime.datetime.now().time() < close_time and datetime.datetime.now().time() > open_time):
while (True):
    data = loader.download()
    # print("Something")
    logging.info("Download done at %s", datetime.datetime.now().time())
    for k, v in data.items():	
        producer.produce('api1', key='key1', value=json.dumps(v))
    producer.flush()
    time.sleep(60)
