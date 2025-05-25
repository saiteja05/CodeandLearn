# Vector Read Benchmarking Framework
### Author: Sai Teja Boddapati

## What Does the Code Do?

This takes in the aggregation pipelines in the **PIPELINES** array and uses the 
vectors in **vectors.csv** to run in multi-threaded fashion using **threads** package
of **Python (Python 3.7 or later)** with request rate as defined in **rpms** array and use
num candidates for the vector search as defined in **num_candidates** array.
All of these attributes are editable.

## Attributes to Replace in Code

```python
uri = "mongodb uri"
db_name = "database name"
coll_name = "collection name"
```

Make sure you include a new **vectors.csv** file as per your request vectors.

Make sure you load some data into MongoDB and create a vector index.
You can learn more about it [here](https://www.mongodb.com/docs/compass/current/indexes/create-vector-search-index/).

Make sure you replace **pipelines** array with the new aggregation pipelines as per the use case.

```python
SAMPLE_SIZE = int(os.getenv('SAMPLE_SIZE', 50))
```

The sample size here represents the random number of vectors from **vectors.csv** to use as request vectors per benchmark run.

You can also alter the details below as necessary; leaving them default will also work:

```python
rpms = [10000, 20000, 50000]
thread_counts = [os.cpu_count(), os.cpu_count()*2, os.cpu_count()*4, 
                os.cpu_count()*8, os.cpu_count()*16, os.cpu_count()*32, 
                os.cpu_count()*64]
num_candidates_list = [10, 20]
```

## Python Packages Needed

```
PyMongo
threading 
statistics
random 
os
time
csv
```

Please use pip to install these packages.

---

#### Trademark MongoDB ©2025
#### MongoDB is not liable for any changes you make to your data using this code. Should be strictly used for benchmarking and testing purposes only.