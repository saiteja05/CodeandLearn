from pymongo import MongoClient, UpdateOne
from bson import Binary
from bson.binary import BinaryVectorDtype
from concurrent.futures import ThreadPoolExecutor
import math

# --- Config ---
MONGO_URI = "mongodb+srv://locust:locust@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=1"
DB_NAME = "ecommerce"
COLLECTION_NAME = "catalog"
BATCH_SIZE = 500000
THREADS = 32


def generate_bson_vector(vector, vector_dtype):
    return Binary.from_vector(vector, vector_dtype)

def process_chunk(docs_chunk, thread_id):
    if not docs_chunk:
        return
    updates = []
    for doc in docs_chunk:
        try:
            embedding = doc.get("embedding")
            if embedding is None:
                continue
            embedding_bin = generate_bson_vector(embedding, BinaryVectorDtype.FLOAT32)
            updates.append(
                UpdateOne({"_id": doc["_id"]}, {"$set": {"embedding_bin": embedding_bin}})
            )
        except Exception as e:
            print(f"[Thread-{thread_id}] Error processing doc {doc.get('_id')}: {e}")

    if updates:
        client = MongoClient(MONGO_URI)
        col = client[DB_NAME][COLLECTION_NAME]
        result = col.bulk_write(updates, ordered=False)
        print(f"[Thread-{thread_id}] Updated {result.modified_count} docs.")

def main():
    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]
    x = 24

    while x>0:
     print("[Main] Fetching documents...")
     docs = list(
            col.find(
                {"embedding_bin": {"$exists": False}},
                {"embedding": 1}
             ).limit(BATCH_SIZE)
            )
     print(f"[Main] Retrieved {len(docs)} docs.")

     # Split into chunks
     chunk_size = math.ceil(len(docs) / THREADS)
     chunks = [docs[i:i + chunk_size] for i in range(0, len(docs), chunk_size)]

     with ThreadPoolExecutor(max_workers=THREADS) as executor:
         for idx, chunk in enumerate(chunks):
             executor.submit(process_chunk, chunk, idx)
     x=x-1

if __name__ == "__main__":
    main()