from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from sentence_transformers import SentenceTransformer
from timeit import default_timer as timer
from bson.binary import Binary
from bson.binary import BinaryVectorDtype

def generate_bson_vector(vector, vector_dtype):
   return Binary.from_vector(vector, vector_dtype)


model = SentenceTransformer(
    'all-MiniLM-L6-v2')  # was trained using cosine similarity and generates 384 dimension dense vector

# uri = "mongodb+srv://tejaboddapati:Bangalore123@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=majority"
uri="mongodb+srv://locust:locust@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"


# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    start = timer()
    # client.admin.command('ping')
    # db=client.KaggleDB
    db = client.sample_mflix
    # dataset=db.movies
    src = db.movies

    # ETL which loads data from sample dataset sample_mflix.movies into sample_mflix.movies_subset
    src.aggregate([{"$match": {"year": {"$gt": 1995}}}, {"$out": "movies_subset1"}])
    dataset = db.movies_subset1
    for i in dataset.find({"fullplot": {"$exists": True}}):
        fullplot_embeddings = model.encode(i['fullplot']).tolist()
        fullplot_embeddings=generate_bson_vector(fullplot_embeddings,BinaryVectorDtype.FLOAT32)
        # print(fullplot_embeddings)
        a = dataset.update_one({"_id": i['_id']}, {"$set": {"fullplot_embeddings": fullplot_embeddings}})
        print(i['_id'])
        print(fullplot_embeddings)
    end = timer()
    print(start)
    print(end)
    print("Successfully loaded embedding in " + str(end - start) + " seconds")
except Exception as e:
    print(e)