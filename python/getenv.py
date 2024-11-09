from pymongo import MongoClient

# Replace <connection-string> with your actual MongoDB connection string
uri = "mongodb+srv://locust:locust@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri)

def get_indexes_and_sharding_keys():
    db_name = "sample_airbnb"
    collection_name = "listingsAndReviews"
    

    databases = client.list_database_names()
    db = client[db_name]
    collection = db[collection_name]

    print(f"Database: {db_name}, Collection: {collection_name}")

    # Get all indexes
    indexes = collection.index_information()
    print('Indexes:', indexes)

    # Check for sharding keys
    admin_db = client['admin']
    shard_info = admin_db.command('listShards')
    shard_keys = []

    for shard in shard_info['shards']:
        chunk_info = client['config']['chunks'].find({'ns': f'{db_name}.{collection_name}', 'shard': shard['_id']})
        for chunk in chunk_info:
            if 'key' in chunk:
                shard_keys.append(chunk['key'])

    if shard_keys:
        print('Sharding Keys:', shard_keys)
    else:
        print('No sharding keys found')

    print('')

if __name__ == "__main__":
    get_indexes_and_sharding_keys()
    client.close()