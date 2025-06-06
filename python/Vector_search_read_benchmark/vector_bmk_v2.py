from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import statistics
import threading

# Global parameters
SAMPLE_SIZE = int(os.getenv('SAMPLE_SIZE', 50))
DURATION_IN_MINUTES = 1
SIMULATE_RPM = False  # full throttle
MAX_POOL_SIZE = 150
MIN_POOL_SIZE = 50

# MongoDB client (global)
mongo_client = None

def get_vectors():
    array_of_arrays = []
    with open('vectors.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) > 0:
                row[0] = row[0].lstrip('[')
                row[-1] = row[-1].rstrip(']')
                vector = [float(value) for value in row]
                array_of_arrays.append(vector)
    return array_of_arrays

# Define the pipeline functions
def search_and_filter_pipeline(queryVector, num_candidates,limit=10):
    return [
        {
            "$vectorSearch": {
                "filter": {
                    "$and": [
                        {"$and": [{"season": "Winter"}, {"$or": [{"gender": "men"}, {"gender": "men women"}]}]},
                        {"$nor": [{"brand": "Nike"}, {"brand": "Franco Leone"}]}
                    ]
                },
                "path": "embedding",
                "index": "vector_index",
                "limit": limit,
                "numCandidates": num_candidates,
                "queryVector": queryVector
            }
        },
        {"$project": {"embedding": 0}}
    ]

def search_and_bucket_pipeline(queryVector, num_candidates,limit=5):
    return [
        {
            "$vectorSearch": {
                "path": "embedding",
                "index": "vector_index",
                "limit": limit,
                "numCandidates": num_candidates,
                "queryVector": queryVector
            }
        },
        {"$project": {"_id": 1, "style_id": 1, "similarity_score": {"$meta": "vectorSearchScore"}}},
        {
            "$bucketAuto": {
                "groupBy": "$similarity_score",
                "buckets": 3,
                "output": {
                    "count": {"$sum": 1},
                    "avgValue": {"$avg": "$similarity_score"},
                    "bucketContents": {"$push": {"_id": "$_id", "style_id": "$style_id", "similarity_score": "$similarity_score"}}
                }
            }
        }
    ]

def search_bucket_sort_pipeline(queryVector, num_candidates,limit=5):
    return [
        {
            "$vectorSearch": {
                "path": "embedding",
                "index": "vector_index",
                "limit": limit,
                "numCandidates": num_candidates,
                "queryVector": queryVector
            }
        },
        {"$project": {"_id": 1, "style_id": 1, "valid_from": 1, "similarity_score": {"$meta": "vectorSearchScore"}}},
        {
            "$bucketAuto": {
                "groupBy": "$similarity_score",
                "buckets": 3,
                "output": {
                    "count": {"$sum": 1},
                    "avgValue": {"$avg": "$similarity_score"},
                    "bucketContents": {
                        "$push": {"_id": "$_id", "style_id": "$style_id", "similarity_score": "$similarity_score", "valid_from": "$valid_from"}
                    }
                }
            }
        },
        {
            "$set": {
                "sorted_desc_by_discounted_price": {
                    "$sortArray": {
                        "input": "$bucketContents",
                        "sortBy": {"discounted_price": 1, "similarity_score": -1}
                    }
                }
            }
        }
    ]

# MongoDB connection initialization
def init_mongo_client(uri):
    global mongo_client
    mongo_client = MongoClient(uri, server_api=ServerApi('1'), maxPoolSize=MAX_POOL_SIZE, minPoolSize=MIN_POOL_SIZE)

def run_pipeline_threading(pipeline, db_name, coll_name):
    global mongo_client
    coll = mongo_client[db_name][coll_name]
    start = time.perf_counter()
    list(coll.aggregate(pipeline))
    duration = (time.perf_counter() - start) * 1000  # convert to ms
    return duration

results_summary = []

def benchmark_all_models(vectors, db_name, coll_name, thread_count, pipeline_name, pipeline_fn, rpm, num_candidates,limit):
    global mongo_client
    try:
        total_requests = rpm * DURATION_IN_MINUTES
        sampled_vectors = random.sample(vectors, SAMPLE_SIZE)
        requested_vectors = (sampled_vectors * ((total_requests // SAMPLE_SIZE) + 1))[:total_requests]
        pipelines = [pipeline_fn(v, num_candidates) for v in requested_vectors]

        durations = []

        def traffic_worker(pipelines_chunk):
            for p in pipelines_chunk:
                dur = run_pipeline_threading(p, db_name, coll_name)
                durations.append(dur)

        chunk_size = (len(pipelines) + thread_count - 1) // thread_count
        threads = []
        for i in range(thread_count):
            chunk = pipelines[i * chunk_size: (i + 1) * chunk_size]
            t = threading.Thread(target=traffic_worker, args=(chunk,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()



        percentiles = {p: statistics.quantiles(durations, n=100)[p - 1] for p in [50, 70, 95, 99]}

        # Get the limit from the first pipeline (as all pipelines will use the same limit)
        limit = pipelines[0][0]['$vectorSearch']['limit'] if pipelines else 0

        print("\n" + "─" * 55)
        print(f"Pipeline:         {pipeline_name}")
        print(f"RPM Target:       {rpm}")
        print(f"Threads:          {thread_count}")
        print(f"Sample size:      {SAMPLE_SIZE}")
        print(f"Request count:    {total_requests}")
        print(f"NumCandidates:    {num_candidates}")
        print(f"Limit:            {limit}")
        for p, val in percentiles.items():
            print(f"P{p} latency:      {val:.2f} ms")

        results_summary.append({
            "pipeline": pipeline_name,
            "nc": num_candidates,
            "limit": limit,
            "rpm": rpm,
            "threads": thread_count,
            "sample": SAMPLE_SIZE,
            "requests": total_requests,
            "p50": percentiles[50],
            "p70": percentiles[70],
            "p95": percentiles[95],
            "p99": percentiles[99]
        })
    except Exception as e:
        print(f"Error while running benchmark: {e}")


def print_final_table():
    if not results_summary:
        print("No results to display.")
        return
    print("\nFINAL SUMMARY TABLE:")
    print(f"Host CPU count {os.cpu_count()}")
    print("=" * 150)
    print(f"{'Pipeline':<40} {'RPM':<8} {'Threads':<8} {'Sample':<8} {'Reqs':<8} {'NumCand':<9} {'Limit':<8} {'P50(ms)':<10} {'P70(ms)':<10} {'P95(ms)':<10} {'P99(ms)':<10}")
    print("=" * 150)
    for r in results_summary:
        print(f"{r['pipeline']:<40} {r['rpm']:<8} {r['threads']:<8} {r['sample']:<8} {r['requests']:<8} {r['nc']:<9} {r['limit']:<8} {r['p50']:<10.2f} {r['p70']:<10.2f} {r['p95']:<10.2f} {r['p99']:<10.2f}")
    print("=" * 150)

if __name__ == '__main__':
    vectors = get_vectors()
    uri = "mongodb+srv://locust:locust@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    db_name = "ecommerce"
    coll_name = "catalog"

    # Initialize MongoDB client once globally
    try:
        init_mongo_client(uri)

        if vectors:
            print("Loaded Vectors")
            rpms = [10000, 20000,50000 ]
            PIPELINES = [
                ("Search and Filter", search_and_filter_pipeline,10),
                ("Search and Bucket (no pre filter)", search_and_bucket_pipeline,5),
                ("Search, Bucket and Sort (no pre filter)", search_bucket_sort_pipeline,5)
            ]

            thread_counts = [os.cpu_count(), os.cpu_count()*2,os.cpu_count()*4,os.cpu_count()*8,os.cpu_count()*16,os.cpu_count()*32,os.cpu_count()*64]

            num_candidates_list = [10, 20]

            for rpm in rpms:
                for threads in thread_counts:
                    for nc in num_candidates_list:
                        for pipeline_name, pipeline_fn,pipeline_limit in PIPELINES:
                            print(f"[INFO] starting benchmark of {pipeline_name} with {threads} threads and will hit {rpm} RPM using {SAMPLE_SIZE} random query vectors , {nc} num candidates and Limit is {pipeline_limit}")
                            benchmark_all_models(vectors, db_name, coll_name, threads, pipeline_name, pipeline_fn, rpm, nc,pipeline_limit)

            print_final_table()

    except Exception as e:
        print(f"Error during MongoDB client initialization or benchmark execution: {e}")
    finally:
        if mongo_client:
            mongo_client.close()  # Ensure connection is closed after the benchmark is complete