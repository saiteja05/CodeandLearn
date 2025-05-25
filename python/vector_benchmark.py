from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import statistics
import psutil
import threading
import queue

SAMPLE_SIZE = int(os.getenv('SAMPLE_SIZE', 50))
DURATION_IN_MINUTES = 1
SIMULATE_RPM = True


def get_vectors():
    array_of_arrays = []
    with open('Vector_search_read_benchmark/vectors.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) > 0:
                row[0] = row[0].lstrip('[')
                row[-1] = row[-1].rstrip(']')
                vector = [float(value) for value in row]
                array_of_arrays.append(vector)
    return array_of_arrays


def functionalAB_pipeline(queryVector, num_candidates):
    return [
        {
            "$vectorSearch": {
                "filter": {
                    "$and": [
                        {
                            "$and": [
                                {"season": "Winter"},
                                {"$or": [
                                    {"gender": "men"},
                                    {"gender": "men women"},
                                ]}
                            ]
                        },
                        {"$nor": [
                            {"brand": "Nike"},
                            {"brand": "Franco Leone"},
                        ]}
                    ]
                },
                "path": "embedding",
                "index": "vector_index",
                "limit": 10,
                "numCandidates": num_candidates,
                "queryVector": queryVector
            }
        },
        {"$project": {"embedding": 0}}
    ]


def functionalC_pipeline(queryVector, num_candidates):
    return [
        {
            "$vectorSearch": {
                "path": "embedding",
                "index": "vector_index",
                "limit": 5,
                "numCandidates": num_candidates,
                "queryVector": queryVector
            }
        },
        {
            "$project": {
                "_id": 1,
                "style_id": 1,
                "similarity_score": {"$meta": "vectorSearchScore"}
            }
        },
        {
            "$bucketAuto": {
                "groupBy": "$similarity_score",
                "buckets": 3,
                "output": {
                    "count": {"$sum": 1},
                    "avgValue": {"$avg": "$similarity_score"},
                    "bucketContents": {
                        "$push": {
                            "_id": "$_id",
                            "style_id": "$style_id",
                            "similarity_score": "$similarity_score"
                        }
                    }
                }
            }
        }
    ]


def functionalD_pipeline(queryVector, num_candidates):
    return [
        {
            "$vectorSearch": {
                "path": "embedding",
                "index": "vector_index",
                "limit": 5,
                "numCandidates": num_candidates,
                "queryVector": queryVector
            }
        },
        {
            "$project": {
                "_id": 1,
                "style_id": 1,
                "valid_from": 1,
                "similarity_score": {"$meta": "vectorSearchScore"}
            }
        },
        {
            "$bucketAuto": {
                "groupBy": "$similarity_score",
                "buckets": 3,
                "output": {
                    "count": {"$sum": 1},
                    "avgValue": {"$avg": "$similarity_score"},
                    "bucketContents": {
                        "$push": {
                            "_id": "$_id",
                            "style_id": "$style_id",
                            "similarity_score": "$similarity_score",
                            "valid_from": "$valid_from"
                        }
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


mongo_client = None

def init_worker(uri):
    mongo_client = MongoClient(uri, server_api=ServerApi('1'))

def run_pipeline_threading(pipeline, db_name, coll_name):

    coll = mongo_client[db_name][coll_name]
    start = time.perf_counter()
    list(coll.aggregate(pipeline))
    duration = time.perf_counter() - start
    return duration

class ResourceMonitor:
    def __init__(self):
        self.max_mem = 0
        self.cpu_samples = []
        self.keep_running = True

    def monitor(self):
        process = psutil.Process(os.getpid())
        while self.keep_running:
            mem = process.memory_info().rss / (1024 * 1024)
            cpu = psutil.cpu_percent(interval=0.2)
            self.cpu_samples.append(cpu)
            self.max_mem = max(self.max_mem, mem)

    def start(self):
        self.thread = threading.Thread(target=self.monitor)
        self.thread.start()

    def stop(self):
        self.keep_running = False
        self.thread.join()
        return self.max_mem, sum(self.cpu_samples)/len(self.cpu_samples) if self.cpu_samples else 0

results_summary = []

def traffic_worker(work_queue, db_name, coll_name, results):
    while True:
        try:
            pipeline = work_queue.get(timeout=1)
        except queue.Empty:
            return
        dur = run_pipeline_threading(pipeline, db_name, coll_name)
        results.append(dur)
        work_queue.task_done()


def benchmark_all_models(vectors, uri, db_name, coll_name, thread_count, pipeline_name, pipeline_fn, rpm, num_candidates):
    total_requests = rpm * DURATION_IN_MINUTES
    sampled_vectors = random.sample(vectors, SAMPLE_SIZE)
    requested_vectors = (sampled_vectors * ((total_requests // SAMPLE_SIZE) + 1))[:total_requests]
    pipelines = [pipeline_fn(v, num_candidates) for v in requested_vectors]
    interval = 60.0 / total_requests if SIMULATE_RPM else 0

    work_q = queue.Queue()
    for p in pipelines:
        work_q.put(p)

    durations = []
    monitor = ResourceMonitor()
    monitor.start()

    def paced_producer():
        start_time = time.perf_counter()
        for i in range(total_requests):
            elapsed = time.perf_counter() - start_time
            expected_time = i * interval
            sleep_time = expected_time - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    threads = []
    for _ in range(thread_count):
        t = threading.Thread(target=traffic_worker, args=(work_q, db_name, coll_name, durations))
        t.start()
        threads.append(t)

    if SIMULATE_RPM:
        paced_producer()

    for t in threads:
        t.join()

    peak_mem, avg_cpu = monitor.stop()
    percentiles = {p: statistics.quantiles(durations, n=100)[p - 1] for p in [50, 70, 95, 99]}

    print("\n" + "─" * 55)
    print(f"Pipeline:         {pipeline_name}-nc{num_candidates}")
    print(f"RPM Target:       {rpm}")
    print(f"Threads:          {thread_count}")
    print(f"Sample size:      {SAMPLE_SIZE}")
    print(f"Request count:    {total_requests}")
    for p, val in percentiles.items():
        print(f"P{p} latency:      {val:.4f}s")
    print(f"Peak Memory:      {peak_mem:.1f}MB")
    print(f"Average CPU:      {avg_cpu:.1f}%")
    print("─" * 55)

    results_summary.append([
        "Threading",
        f"{pipeline_name}-nc{num_candidates}",
        thread_count,
        total_requests,
        SAMPLE_SIZE,
        percentiles[50],
        percentiles[70],
        percentiles[95],
        percentiles[99],
        peak_mem,
        avg_cpu
    ])


def print_summary_table():
    print("\nFinal Summary Table:")
    print("| Method      | Pipeline             | Threads | Req Count | Sample Size |  P50   |  P70   |  P95   |  P99   | Mem(MB) | CPU(%) |")
    print("|-------------|----------------------|---------|-----------|-------------|--------|--------|--------|--------|---------|--------|")
    for row in results_summary:
        print(f"| {row[0]:<11} | {row[1]:<20} | {row[2]:>7} | {row[3]:>9} | {row[4]:>11} | {row[5]:.4f} | {row[6]:.4f} | {row[7]:.4f} | {row[8]:.4f} | {row[9]:>7.1f} | {row[10]:>6.1f} |")


if __name__ == '__main__':
    vectors = get_vectors()
    uri = "mongodb+srv://locust:locust@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    db_name = "ecommerce"
    coll_name = "catalog"

    if vectors:
        cpucount = int(os.cpu_count())
        PROCESS_COUNTS = [int(cpucount / 2), cpucount, cpucount * 2]
        RPM_TARGETS = [10000, 20000]
        PIPELINES = [
            ("functionalAB", functionalAB_pipeline),
            ("functionalC", functionalC_pipeline),
            ("functionalD", functionalD_pipeline)
        ]

        init_worker(uri)
        for rpm in RPM_TARGETS:
            for thread_count in PROCESS_COUNTS:
                for pipeline_name, pipeline_fn in PIPELINES:
                    for num_candidates in [10, 20]:
                        print(
                            f"[INFO] starting benchmark of {pipeline_name} with {thread_count} threads and will hit {rpm} RPM using {SAMPLE_SIZE} random query vectors")

                        benchmark_all_models(vectors, uri, db_name, coll_name, thread_count, pipeline_name, pipeline_fn, rpm, num_candidates)

        print_summary_table()

