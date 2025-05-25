# MongoDB Write Benchmark Tool
### Author: Sai Teja Boddapati

## What Does the Code Do?

This code provides detailed benchmarking for batch writes using MongoDB's `insert_many` function. It takes into consideration the underlying number of CPUs, document size, and the number of documents you want to insert. The code is capable of generating test data automatically, so you don't need to provide any data, but you do need to specify the document size you want to benchmark with.

## Attributes to Replace in Code

### Basic Connection Settings
```python
MONGO_URI = "endpoint"
DATABASE_NAME = "benchmark_db"
COLLECTION_NAME = "benchmark_collection2"
```
These are basic connection attributes you need to modify.

### Benchmark Configuration
```python
DOC_COUNT = 100000  # Total number of documents for each batch benchmark
DOC_SIZE = 1 * 1024  # Size of each document in bytes
cpucount = int(os.cpu_count())
PROCESS_COUNTS = [int(cpucount / 2), cpucount, cpucount * 2, cpucount * 3, cpucount * 4]
```

You can alter the above attributes to modify various properties of the benchmark.

## ⚠️ Important Warning

```python
print("Dropping benchmark collection")
client[DATABASE_NAME][COLLECTION_NAME].drop()
client.close()
```

**Please note:** The current code deletes the collection it inserts data into after the benchmark completes. **Do not point it to a collection whose data is essential** as all data will be permanently lost.

## Python Packages Needed

```
PyMongo
multiprocessing 
statistics
random 
os
time
csv
tqdm
```

Please use pip to install these packages.

## Sample Output

```
===== Summary of Benchmarks using insert_many =====
cpu count : 12
Processes      Batch Size     Doc Size (KB)  TPS            Total Time (s)      Successful Inserts  Failed Inserts 
----------------------------------------------------------------------------------------------------
6              100            1              757.56         132.00              99996               0              
12             100            1              1308.99        76.39               99996               0              
24             100            1              2391.47        41.81               99984               0              
36             100            1              3150.15        31.74               99972               0              
48             100            1              3893.89        25.68               99984               0       
```

The output shows:
- **Processes**: Number of concurrent processes used
- **Batch Size**: Documents per batch insert operation
- **Doc Size (KB)**: Size of each document in kilobytes
- **TPS**: Transactions per second (throughput)
- **Total Time (s)**: Total benchmark execution time
- **Successful/Failed Inserts**: Count of successful and failed insert operations

---

#### Trademark MongoDB ©2025
#### MongoDB is not liable for any changes you make to your data using this code. Should be strictly used for benchmarking and testing purposes only.