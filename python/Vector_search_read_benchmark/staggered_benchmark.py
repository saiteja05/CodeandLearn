from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import ConnectionFailure, OperationFailure
import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import statistics
import threading
import traceback
from threading import Lock
import queue
import signal
import sys

# Global parameters
SAMPLE_SIZE = int(os.getenv('SAMPLE_SIZE', 50))
DURATION_IN_MINUTES = 1
MAX_POOL_SIZE = 150
MIN_POOL_SIZE = 50
RETRY_COUNT = 3  # Number of retries for failed operations
RETRY_DELAY = 1.0  # Seconds between retries

# MongoDB client (global)
mongo_client = None
results_summary = []
results_lock = Lock()  # Lock for thread-safe access to results_summary

def get_vectors():
    """
    Read vectors from CSV file for benchmarking.
    Returns:
        List of vector arrays (list of lists of floats)
    """
    array_of_arrays = []
    try:
        # Use absolute path to ensure we find the file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        vectors_path = os.path.join(script_dir, 'vectors.csv')
        print(f"Looking for vectors at: {vectors_path}")
        
        with open(vectors_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) > 0:
                    row[0] = row[0].lstrip('[')
                    row[-1] = row[-1].rstrip(']')
                    vector = [float(value) for value in row]
                    array_of_arrays.append(vector)
        print(f"Loaded {len(array_of_arrays)} vectors from vectors.csv")
        return array_of_arrays
    except Exception as e:
        print(f"Error loading vectors: {e}")
        traceback.print_exc()  # Print the full stack trace for better debugging
        return []

# Define the pipeline functions
def search_and_filter_pipeline(queryVector, num_candidates, limit=10):
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

def search_and_bucket_pipeline(queryVector, num_candidates, limit=5):
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

def search_bucket_sort_pipeline(queryVector, num_candidates, limit=5):
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

def functionalHybrid_pipeline(queryVector, num_candidates, limit):
    return [
        {
            "$rankFusion": {
                "input": {
                    "pipelines": {
                        "searchOne": [
                            {
                                "$vectorSearch": {
                                    "index": "vector_index",
                                    "queryVector": queryVector,
                                    "path": "embedding",
                                    "numCandidates": num_candidates,
                                    "limit": limit,
                                    "compound": {
                                        "must": [
                                            {
                                                "equals": {
                                                    "path": "brands_filter_facet",
                                                    "value": "Titan"
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        ],
                        "searchTwo": [
                            {
                                "$search": {
                                    "index": "ecom_search",
                                    "compound": {
                                        "must": [
                                            {
                                                "compound": {
                                                    "should": [
                                                        {
                                                            "text": {
                                                                "query": "women",
                                                                "path": "global_attr_gender"
                                                            }
                                                        },
                                                        {
                                                            "text": {
                                                                "query": "dress",
                                                                "path": "global_attr_sub_category"
                                                            }
                                                        },
                                                        {
                                                            "text": {
                                                                "query": "dresses",
                                                                "path": "global_attr_article_type"
                                                            }
                                                        },
                                                        {
                                                            "text": {
                                                                "query": "yellow",
                                                                "path": "global_attr_base_colour"
                                                            }
                                                        },
                                                        {
                                                            "text": {
                                                                "query": "maxi",
                                                                "path": "Length_article_attr"
                                                            }
                                                        }
                                                    ]
                                                }
                                            },
                                            {
                                                "equals": {
                                                    "path": "brands_filter_facet",
                                                    "value": "Titan"
                                                }
                                            }
                                        ]
                                    }
                                }
                            },
                            {
                                "$limit": limit
                            }
                        ]
                    }
                },
                "scoreDetails": True
            }
        },
        {
            "$limit": num_candidates
        },
        {
            "$project": {
                "_id": 1,
                "score": {
                    "$meta": "vectorSearchScore"
                }
            }
        }
    ]

def vectoronly_pipeline(queryVector, num_candidates, limit):
    return [
        {
            "$vectorSearch": {
                "index": "vector_index",
                "queryVector": queryVector,
                "path": "embedding",
                "numCandidates": num_candidates,
                "limit": limit,
                "compound": {
                    "must": [
                        {
                            "equals": {
                                "path": "brands_filter_facet",
                                "value": "Titan"
                            }
                        }
                    ]
                }
            }
        },
        {"$project": {"_id": 1, "style_id": 1, "similarity_score": {"$meta": "vectorSearchScore"}}}
    ]

# MongoDB connection initialization
def init_mongo_client(uri):
    """
    Initialize MongoDB client with connection pooling and retries
    """
    global mongo_client
    for attempt in range(RETRY_COUNT):
        try:
            mongo_client = MongoClient(
                uri, 
                server_api=ServerApi('1'), 
                maxPoolSize=MAX_POOL_SIZE, 
                minPoolSize=MIN_POOL_SIZE,
                connectTimeoutMS=5000,
                socketTimeoutMS=30000,
                waitQueueTimeoutMS=10000,
                retryWrites=True
            )
            # Test the connection
            mongo_client.admin.command('ping')
            print("✅ MongoDB connection established successfully")
            return True
        except ConnectionFailure as e:
            if attempt < RETRY_COUNT - 1:
                print(f"MongoDB connection attempt {attempt+1} failed: {e}. Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"❌ Failed to connect to MongoDB after {RETRY_COUNT} attempts: {e}")
                return False

def run_pipeline_threading(pipeline, db_name, coll_name):
    """
    Run a MongoDB pipeline with proper error handling and retries
    
    Args:
        pipeline: MongoDB aggregation pipeline
        db_name: Database name
        coll_name: Collection name
        
    Returns:
        Duration in milliseconds or None if operation failed
    """
    global mongo_client
    
    for attempt in range(RETRY_COUNT):
        try:
            coll = mongo_client[db_name][coll_name]
            start = time.perf_counter()
            list(coll.aggregate(pipeline))
            duration = (time.perf_counter() - start) * 1000  # convert to ms
            return duration
        except (ConnectionFailure, OperationFailure) as e:
            if attempt < RETRY_COUNT - 1:
                print(f"Pipeline execution attempt {attempt+1} failed: {e}. Retrying...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Pipeline execution failed after {RETRY_COUNT} attempts: {e}")
                return None

class BenchmarkManager:
    """
    Thread-safe manager for running benchmarks with proper synchronization
    """
    def __init__(self, vectors, db_name, coll_name):
        self.vectors = vectors
        self.db_name = db_name
        self.coll_name = coll_name
        self.running = False
        self.shutdown_event = threading.Event()
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    def handle_signal(self, signum, frame):
        """Signal handler for graceful shutdown"""
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.shutdown_event.set()
    
    def benchmark_all_models(self, thread_count, pipeline_name, pipeline_fn, rpm, num_candidates, limit):
        """
        Run a benchmark with the specified parameters using thread-safe operations
        
        Args:
            thread_count: Number of concurrent threads
            pipeline_name: Name of the pipeline for reporting
            pipeline_fn: Function that creates the pipeline
            rpm: Target requests per minute
            num_candidates: Number of candidates for vector search
            limit: Limit for results
        """
        global mongo_client, results_summary
        
        # Mark as running
        self.running = True
        
        try:
            # Calculate total requests
            total_requests = rpm * DURATION_IN_MINUTES
            
            # Prepare test data
            sample_size = min(SAMPLE_SIZE, len(self.vectors))
            sampled_vectors = random.sample(self.vectors, sample_size)
            requested_vectors = (sampled_vectors * ((total_requests // sample_size) + 1))[:total_requests]
            pipelines = [pipeline_fn(v, num_candidates, limit) for v in requested_vectors]
            
            # Thread synchronization
            durations_queue = queue.Queue()
            error_count = 0
            error_lock = Lock()
            
            # Rate limiting setup
            enable_rate_limiting = rpm < 10000  # Only rate limit for lower RPMs
            request_interval = 60.0 / rpm if enable_rate_limiting else 0
            start_time = time.perf_counter()
            next_request_time = start_time
            next_time_lock = Lock()
            
            # Monitor thread
            self.shutdown_event.clear()
            
            def process_pipeline(pipeline_idx, pipeline):
                """Worker function to process a single pipeline with rate limiting"""
                nonlocal error_count, next_request_time
                
                # Check for shutdown signal
                if self.shutdown_event.is_set():
                    return "Shutdown requested"
                
                # Rate limiting
                if enable_rate_limiting:
                    with next_time_lock:
                        scheduled_time = next_request_time
                        next_request_time = scheduled_time + request_interval
                    
                    wait_time = scheduled_time - time.perf_counter()
                    if wait_time > 0:
                        time.sleep(wait_time)
                
                # Run the query
                try:
                    duration = run_pipeline_threading(pipeline, self.db_name, self.coll_name)
                    if duration is not None:
                        durations_queue.put(duration)
                    else:
                        with error_lock:
                            error_count += 1
                        return f"Error on request {pipeline_idx}: Duration is None"
                    return None
                except Exception as e:
                    with error_lock:
                        error_count += 1
                    return f"Error on request {pipeline_idx}: {str(e)}"
            
            # Progress tracking
            total_count = len(pipelines)
            completed_count = 0
            last_progress_time = time.time()
            progress_interval = 5  # seconds between progress updates
            
            def progress_monitor():
                """Monitor and report progress periodically"""
                nonlocal completed_count, last_progress_time
                
                while not self.shutdown_event.is_set() and completed_count < total_count:
                    current_time = time.time()
                    if current_time - last_progress_time >= progress_interval:
                        progress_pct = (completed_count / total_count) * 100
                        print(f"Progress: {completed_count}/{total_count} ({progress_pct:.1f}%)")
                        last_progress_time = current_time
                    time.sleep(1)
            
            # Start progress monitor
            monitor_thread = threading.Thread(target=progress_monitor)
            monitor_thread.daemon = True
            monitor_thread.start()
            
            # Use ThreadPoolExecutor for better thread management
            print(f"Starting benchmark with {thread_count} threads, targeting {rpm} RPM...")
            errors = []
            
            completed_count = 0
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                # Submit all tasks
                future_to_idx = {executor.submit(process_pipeline, i, p): i for i, p in enumerate(pipelines)}
                
                # Process results as they complete
                for future in as_completed(future_to_idx):
                    completed_count += 1
                    error = future.result()
                    if error:
                        errors.append(error)
                        if len(errors) <= 5:  # Limit error reporting
                            print(error)
                    
                    # Check for shutdown
                    if self.shutdown_event.is_set():
                        print("Shutdown detected, cancelling pending tasks...")
                        for f in future_to_idx:
                            if not f.done():
                                f.cancel()
                        break
            
            # Collect all durations from the queue
            durations = []
            while not durations_queue.empty():
                durations.append(durations_queue.get())
            
            # Calculate statistics
            if durations:
                # Calculate percentiles
                percentiles = {p: statistics.quantiles(durations, n=100)[p - 1] for p in [50, 70, 95, 99]}
                
                # Calculate actual RPM
                execution_time_minutes = (time.perf_counter() - start_time) / 60
                actual_rpm = len(durations) / (execution_time_minutes if execution_time_minutes > 0 else DURATION_IN_MINUTES)
                
                # Print results
                print("\n" + "─" * 55)
                print(f"Pipeline:         {pipeline_name}")
                print(f"RPM Target:       {rpm}")
                print(f"RPM Actual:       {actual_rpm:.2f}")
                print(f"Threads:          {thread_count}")
                print(f"Sample size:      {sample_size}")
                print(f"Completed:        {len(durations)}/{total_requests} ({len(durations)/total_requests*100:.1f}%)")
                print(f"Errors:           {error_count}")
                print(f"NumCandidates:    {num_candidates}")
                print(f"Limit:            {limit}")
                print(f"Execution time:   {execution_time_minutes:.2f} minutes")
                
                for p, val in percentiles.items():
                    print(f"P{p} latency:      {val:.2f} ms")
                
                # Save results for final table
                with results_lock:  # Thread-safe access to results_summary
                    results_summary.append({
                        "pipeline": pipeline_name,
                        "num_candidates": num_candidates,
                        "TopK": limit,
                        "rpm": rpm,
                        "actual_rpm": round(actual_rpm, 1),
                        "threads": thread_count,
                        "sample": sample_size,
                        "requests": total_requests,
                        "completed": len(durations),
                        "errors": error_count,
                        "p50": percentiles[50],
                        "p70": percentiles[70],
                        "p95": percentiles[95],
                        "p99": percentiles[99]
                    })
            else:
                print(f"ERROR: No successful requests completed for {pipeline_name}")
                
        except Exception as e:
            print(f"Error in benchmark_all_models: {e}")
            traceback.print_exc()
        finally:
            self.running = False

def print_final_table():
    """
    Print a formatted table with all benchmark results
    """
    if not results_summary:
        print("No results to display.")
        return
    
    print("\nFINAL SUMMARY TABLE:")
    print(f"Host CPU count: {os.cpu_count()}")
    print("=" * 170)
    print(f"{'Pipeline':<30} {'RPM Target':<10} {'RPM Actual':<10} {'Threads':<8} {'Sample':<8} {'Reqs':<8} {'Completed':<10} {'Errors':<8} {'NumCand':<9} {'TopK':<8} {'P50(ms)':<10} {'P70(ms)':<10} {'P95(ms)':<10} {'P99(ms)':<10}")
    print("=" * 170)
    
    for r in results_summary:
        # Get completed percentage
        completed_pct = (r['completed'] / r['requests']) * 100 if r['requests'] > 0 else 0
        
        print(f"{r['pipeline']:<30} {r['rpm']:<10} {r.get('actual_rpm', 0):<10.1f} {r['threads']:<8} {r['sample']:<8} {r['requests']:<8} {r['completed']:<6}({completed_pct:.1f}%) {r.get('errors', 0):<8} {r['num_candidates']:<9} {r['TopK']:<8} {r['p50']:<10.2f} {r['p70']:<10.2f} {r['p95']:<10.2f} {r['p99']:<10.2f}")
    
    print("=" * 170)

if __name__ == '__main__':
    vectors = get_vectors()
    uri = os.getenv('MONGODB_URI', "mongodb+srv://locust:locust1@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=majority&readPreference=nearest&appName=Cluster0")
    db_name = os.getenv('MONGODB_DB', "ecommerce")
    coll_name = os.getenv('MONGODB_COLL', "catalog")

    # Initialize MongoDB client once globally
    if not init_mongo_client(uri):
        print("Exiting due to MongoDB connection failure")
        sys.exit(1)

    if not vectors:
        print("No vectors loaded. Exiting.")
        sys.exit(1)

    try:
        print(f"Starting benchmarks with vectors loaded")
        
        # Configure the tests
        rpms = [100, 1000, 10000, 50000, 100000]
        
        # Choose which pipelines to benchmark
        PIPELINES = [
            ("functionalHybrid", functionalHybrid_pipeline), 
            ("VectorSearchOnly", vectoronly_pipeline)
        ]
        
        # Define thread counts - dynamic based on CPU count
        cpu_count = os.cpu_count()
        thread_counts = [cpu_count, cpu_count*2, cpu_count*4, cpu_count*8]
        
        # Define candidate counts and limits
        num_candidates_list = [(100, 100), (300, 100), (500, 100), (500, 250), (500, 500)]
        
        # Create the benchmark manager
        benchmark_mgr = BenchmarkManager(vectors, db_name, coll_name)
        
        for rpm in rpms:
            for pipeline_name, pipeline_fn in PIPELINES:
                for threads in thread_counts:
                    # Only run if we have enough threads for the RPM
                    if rpm / 2 > threads:
                        for nc, limit in num_candidates_list:
                            # Dynamically adjust sample size for larger workloads
                            factor = int((float(rpm) * 0.1) % 500)
                            current_sample_size = factor if factor > 50 else SAMPLE_SIZE
                            
                            # Update the global variable (thread-safe since we're still in setup)
                            SAMPLE_SIZE = current_sample_size
                            
                            print(f"\n[INFO] Starting benchmark of {pipeline_name}")
                            print(f"       Threads: {threads}")
                            print(f"       Target RPM: {rpm}")
                            print(f"       Sample size: {SAMPLE_SIZE}")
                            print(f"       Num candidates: {nc}")
                            print(f"       Limit: {limit}")
                            
                            # Run the benchmark
                            benchmark_mgr.benchmark_all_models(
                                threads, pipeline_name, pipeline_fn, rpm, nc, limit
                            )

        # Print the final summary table
        print_final_table()

    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user. Generating report with completed tests...")
        print_final_table()
    except Exception as e:
        print(f"Error during benchmark execution: {e}")
        traceback.print_exc()
    finally:
        if mongo_client:
            mongo_client.close()
            print("MongoDB connection closed")
