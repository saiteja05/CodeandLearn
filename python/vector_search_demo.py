from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from sentence_transformers import SentenceTransformer
import sys

# total arguments
n = len(sys.argv)
print("len",n)
query = sys.argv[1]
model = SentenceTransformer('all-MiniLM-L6-v2')
uri = "mongodb+srv://tejaboddapati:Bangalore123@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(uri, server_api=ServerApi('1'))

try:
    query_embedding = model.encode(query).tolist()
    agg = [
        {
            "$vectorSearch": {
                "index": "vector_index",
                "path": "fullplot_embeddings",
                "queryVector": query_embedding,
                "numCandidates": 20,
                "limit": 5
            }
        }
    ]
    db = client.sample_mflix
    dataset = db.movies_subset
    outDocs = dataset.aggregate(agg)
    for i in outDocs:
        print(i['title'])
except Exception as e:
    print(e)
