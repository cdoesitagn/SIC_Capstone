import pymongo
from pymongo import MongoClient
# Assuming youre running mongod on 'localhost' with port 27017
c = MongoClient("100.89.103.30:27017", 27017)
# myclient = pymongo.MongoClient("mongodb://localhost:27017/")
print(c.list_database_names())
