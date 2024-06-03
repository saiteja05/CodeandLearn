from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from sentence_transformers import SentenceTransformer
import sys

# total arguments
# n = len(sys.argv)
# print("len",n)
query = "a superhero movie"
model = SentenceTransformer('all-MiniLM-L6-v2')
uri="mongodb+srv://tejaboddapati:Bangalore123@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"


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
                ,"filter":{"year":{"$gte":2015}}
            }
        }
    ]
    db = client.sample_mflix
    dataset = db.movies_subset
    #running the vector search
    outDocs = dataset.aggregate(agg)

    if outDocs is not None:
        print("Output is not null proceed to LLM")
    else:
        print("no Output Exists")

    for i in outDocs:
        print(i['title'])
except Exception as e:
    print(e)
