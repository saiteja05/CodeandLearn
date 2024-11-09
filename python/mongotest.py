
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://locust:locust@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=majority&readPreference=secondaryPreferred&maxStalenessSeconds=120"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
# try:
#     client.admin.command('ping')
#     print("Pinged your deployment. You successfully connected to MongoDB!")
# except Exception as e:
#     print(e)

try:
    db=client["Sizing"]
    col=db["Transactions"]
    i=0
    while(i<1):
        i=i+1
        c=col.delete_one({'account_id':{'$gt':0}})
        print(c)

except Exception as e:
    print(e)
    e.__getstate__()