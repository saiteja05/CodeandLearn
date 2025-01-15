import sys
from multiprocessing import Process, Manager
import time
from pymongo import MongoClient
import random
import string
import os
from tqdm import tqdm

MONGO_URI = "mongodb+srv://locust:locust@cluster2.tcgzn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster2"
DATABASE_NAME = "benchmark_db"
COLLECTION_NAME = "benchmark_collection2"
DOC_COUNT = 10000  # Total number of documents for each test
DOC_SIZE = 5 * 1024  # Size of each document in bytes
cpucount = int(os.cpu_count())
PROCESS_COUNTS = [int(cpucount / 2), cpucount, cpucount * 2, cpucount * 3, cpucount * 4]

# Determine batch size based on document size
def get_batch_size(doc_size):
    if doc_size <= 100 * 1024:  # 0–100 KB
        return 5000
    elif doc_size <= 500 * 1024:  # 100–500 KB
        return 1000
    else:  # > 500 KB
        return 200

# Generate a random document of specified size
def generate_document():
    random_text = ''.join(random.choices(string.ascii_letters + string.digits, k=DOC_SIZE - 100))
    return {"content": random_text, "metadata": {"timestamp": time.time(), "source": "benchmark"}}

# Insert documents in batches
def insert_documents(process_id, docs, success_counter, fail_counter, batch_size):
    client = MongoClient(MONGO_URI)
    collection = client[DATABASE_NAME][COLLECTION_NAME]
    success = 0
    fail = 0

    for i in range(0, len(docs), batch_size):
        batch = docs[i:i + batch_size]
        try:
            collection.insert_many(batch, ordered=False)
            success += len(batch)
        except Exception as e:
            fail += len(batch)
            print(f"Process {process_id}: Error during batch insert - {e}")

    success_counter[process_id] = success
    fail_counter[process_id] = fail
    client.close()

# Benchmark logic using multiprocessing
def run_benchmark(process_count, all_documents, batch_size):
    docs_per_process = DOC_COUNT // process_count

    # Split documents among processes
    split_docs = [all_documents[i * docs_per_process: (i + 1) * docs_per_process] for i in range(process_count)]

    processes = []
    manager = Manager()
    success_counter = manager.dict()
    fail_counter = manager.dict()

    # Drop collection before starting
    client = MongoClient(MONGO_URI)
    if COLLECTION_NAME in client[DATABASE_NAME].list_collection_names():
        client[DATABASE_NAME][COLLECTION_NAME].drop()
        print(f"Dropped existing collection: {COLLECTION_NAME}")

    # Start processes and time measurement
    start_time = time.time()
    for i in range(process_count):
        process = Process(target=insert_documents, args=(i, split_docs[i], success_counter, fail_counter, batch_size))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    end_time = time.time()

    # Collect results
    total_success = sum(success_counter.values())
    total_failed = sum(fail_counter.values())
    total_time = end_time - start_time
    tps = total_success / total_time

    print("Dropping benchmark collection")
    client[DATABASE_NAME][COLLECTION_NAME].drop()
    client.close()

    # Return benchmark results
    return {
        "process_count": process_count,
        "doc_size_kb": DOC_SIZE // 1024,
        "total_documents": DOC_COUNT,
        "total_time": total_time,
        "tps": tps,
        "successful_inserts": total_success,
        "failed_inserts": total_failed,
        "batch_size": batch_size
    }

# Main function to run benchmarks for multiple process counts
def benchmark_mongodb_multiple_processes():
    # Generate all documents upfront
    all_documents = [generate_document() for _ in tqdm(range(DOC_COUNT), desc="Document Generation")]

    # Determine batch size based on document size
    batch_size = get_batch_size(DOC_SIZE)

    summary = []

    for process_count in PROCESS_COUNTS:
        print(f"\nRunning benchmark with {process_count} processes and batch size {batch_size}...")
        result = run_benchmark(process_count, all_documents, batch_size)
        summary.append(result)
        print(f"Completed benchmark with {process_count} processes.")
        print(f"Transactions Per Second (TPS): {result['tps']:.2f}")
        print(f"Total Time: {result['total_time']:.2f} seconds\n")

    # Print summary
    print("\n===== Summary of Benchmarks using insert_many =====")
    print(f"cpu count : {cpucount}")
    print(f"{'Processes':<15}{'Batch Size':<15}{'Doc Size (KB)':<15}{'TPS':<15}{'Total Time (s)':<20}{'Successful Inserts':<20}{'Failed Inserts':<15}")
    print("-" * 100)
    for result in summary:
        print(f"{result['process_count']:<15}{result['batch_size']:<15}{result['doc_size_kb']:<15}{result['tps']:<15.2f}{result['total_time']:<20.2f}{result['successful_inserts']:<20}{result['failed_inserts']:<15}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        DOC_COUNT = int(sys.argv[1])
    if len(sys.argv) > 2:
        DOC_SIZE = int(sys.argv[2]) * 1024
    benchmark_mongodb_multiple_processes()