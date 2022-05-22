from pymongo     import MongoClient
import json 

def saveToMongo(databaseName, collName, data):  
    clt  = MongoClient('localhost', 27017)
    db   = clt[databaseName]
    coll = db[collName]
    coll.insert_many(data)

def loadTweets():
    with open("collectedtweets.txt", 'r', encoding="utf-8") as f:
        data = [json.loads(l) for l in f.readlines() if len(l) > 5]
    return data
   
if __name__ == '__main__':    
    data = loadTweets()
    saveToMongo("databaseName", "collectionName", data)