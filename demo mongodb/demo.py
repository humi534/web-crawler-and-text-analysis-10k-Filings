#------------------------------------------------------
#               HELLO WORLD
#------------------------------------------------------

#Load and connect to mongoDB
from pymongo import MongoClient
client = MongoClient('localhost', 27017)

#Create a database and collection inside it
db = client.test_database
collection = db.test_collection_1

#Create a datapoint
datapoint = {"name": "Judicael",
       "age": 23,
       "text": "My first datapoint",
       "tags": ["mongodb", "python", "pymongo"]}
import pprint
print("Datapoint to insert : ")
pprint.pprint(datapoint)

#Insert datapoint in collection
collection.insert_one(datapoint)

#Get one datapoint back
print("Datapoint in collection : ")
pprint.pprint(collection.find_one())

#See all collections
print("collections : ",db.list_collection_names())

#destroy collection
collection.drop()

#Create a new collection
collection = db.test_collection_2

#Create many datapoints
import random
customers = [{"customerID":i,"moneySpent":random.randrange(100)} 
                for i in range(10)]

#Insert all customers
collection.insert_many(customers)

#Count all customers back
print("#customer in collection : ",collection.count_documents({}))

#Count customers with moneySpent > 50
print("#customer in collection with moneySpent > 50 : ",collection.count_documents({"moneySpent": {"$gt": 50}}))

#Get customers id only with moneySpent > 50
cursor = collection.find({"moneySpent": {"$gt": 50}}, {"customerID":1})
print("Results of find : ")
for document in cursor: 
    pprint.pprint(document)

#add 100 to moneySpent to each customer with moneySpent > 50
result = collection.update_many({"moneySpent": {"$gt": 50}}, {'$inc': {'moneySpent': 100}})
print("#customer who gained 100 money : ",result.modified_count)

#Count customers with moneySpent > 170
print("#customer with moneySpent > 170 : ",collection.count_documents({"moneySpent": {"$gt": 170}}))

#Delete customers with moneySpent < 25
result = collection.delete_many({"moneySpent": {"$lte": 25}})
print("#customer with moneySpent < 25 who were deleted: ",result.deleted_count)

#Count all customers
print("#customer in collection : ",collection.count_documents({}))

#count customer with moneySpent < 50 or moneySpent > 170
print("#customer with moneySpent > 170 or < 50 : ",collection.count_documents({ "$or": [ { "moneySpent": {"$gt": 170} },{ "moneySpent": {"$lt": 50} } ] }))

#add field goodCustomer = True to each customer with moneySpent > 170
result = collection.update_many({"moneySpent": {"$gt": 170}}, {'$set': {'goodCustomer': True}})
print("#customer who were labelled goodCustomer: ",result.modified_count)

#Count all customers who are goodCustomer 
print("#goodCustomer: ",collection.count_documents({ "goodCustomer": { "$exists": True } }))

#destroy collection
collection.drop()











