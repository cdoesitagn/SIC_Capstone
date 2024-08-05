from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import requests
import json
import time
import datetime
import logging
import threading
import src.data as dt


kafka_config = {
    'bootstrap.servers': 'localhost:9092'
}

# Create AdminClient
admin_client = AdminClient(kafka_config)
# Fetch metadata
metadata = admin_client.list_topics(timeout=10)
# Get and print all topics
topics = metadata.topics
# Kafka Producer Configuration
log_filename = f"logs/{datetime.datetime.now().strftime('%Y-%m-%d')}_producerRT_script.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)

icb_symbol = json.loads(open('assets/icb_symbol.json', 'r').read())
topic_names = list(icb_symbol.keys())

for t in topic_names:
    if t in list(topics.keys()):
        continue
    else:
        print(t, "is not in topics")

day_date_formated = "2024-08-05"

def kafka_producer_thread(producer_id, topic_name):
    producer = Producer(kafka_config)
    tickers = icb_symbol[topic_name]
    loader = dt.DataLoader_json(symbols=tickers,
           start=day_date_formated,
           end=day_date_formated,
           minimal=False,
           data_source="cafe",
           table_style="prefix")
    while (True):
        data = loader.download()
        logging.info("Download done at %s", datetime.datetime.now().time())
        for k, v in data.items():	
            producer.produce(topic_name, value=json.dumps(v))
        producer.flush()
        time.sleep(60)


threads = []
for i, topic_name in enumerate(topic_names):
    thread = threading.Thread(target=kafka_producer_thread, args=(i, topic_name))
    threads.append(thread)
    thread.start()

# Join threads to the main thread
for thread in threads:
    thread.join()
# tickers = ['BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 'MBB', 'MSN', 'MWG', 'PLX', 'POW', 'SAB', 'SHB', 'SSB', 'SSI', 'STB', 'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE']
# open_time = datetime.time(9, 15)
# close_time = datetime.time(15, 0)
# day_date = datetime.datetime.today().date()
# # day_date_formated = day_date.strftime('%Y-%m-%d')
# day_date_formated = "2024-08-05"
# loader = dt.DataLoader_json(symbols=tickers,
#            start=day_date_formated,
#            end=day_date_formated,
#            minimal=False,
#            data_source="cafe",
#            table_style="prefix")
# # while (datetime.datetime.now().time() < close_time and datetime.datetime.now().time() > open_time):
# while (True):
#     data = loader.download()
#     logging.info("Download done at %s", datetime.datetime.now().time())
#     for k, v in data.items():	
#         producer.produce('api1', key='key1', value=json.dumps(v))
#     producer.flush()
#     time.sleep(60)
