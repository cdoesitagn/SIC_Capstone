from confluent_kafka import Consumer, KafkaError
import pymongo
from pymongo import MongoClient
from bson import json_util
import json
import datetime
import logging
log_filename = f"logs/{datetime.datetime.now().strftime('%Y-%m-%d')}_consumerEOD_script.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)
# Assuming youre running mongod on 'localhost' with port 27017
c = MongoClient("100.89.103.30:27017", 27017)
db = c["stockDB"]
eodPrices = db.eodPrices
# Kafka Consumer Configuration
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'
})

# Subscribe to the Kafka topics
consumer.subscribe(['api1'])

# while True:
message = consumer.poll(1.0)  # Adjust the timeout as needed
# if message is None:
#     continue
if message.error():
    if message.error().code() == KafkaError._PARTITION_EOF:
        print(f"Reached end of partition: {message.topic()} [{message.partition()}]")
    else:
        print(f"Error while consuming from topic {message.topic()}: {message.error()}")
else:
    data = json.loads(message.value().decode('utf-8'))
    print(f"Received message at", datetime.datetime.now().time())
    eodPrices.insert_many(data)
    logging.info("Insert data done at %s ", datetime.datetime.now().time())
        # print("Insert Done \n\n")
# Close the Kafka consumer gracefully when done
consumer.close()
