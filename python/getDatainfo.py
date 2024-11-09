from pymongo import MongoClient

# uri = "mongodb+srv://<>:<>@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
uri = ""

try:
    client = MongoClient(uri)
    # for config databases
    config_db = client.config

    # List Databases
    for db_name in client.list_database_names():
        if db_name in ["admin", "local", "config"]:
            print("Skipping internal database:", db_name)
            continue

        print("database:", db_name)
        print("==================")
        db = client[db_name]
        for cname in db.list_collections():
            col_name = cname['name']
            print("collection: ", col_name)
            print("Total Document Count:", db[(cname['name']) + ""].count_documents({}))
            stats = db.command("collstats", collection_name)
            data_size = stats.get("size", 0)
            print("size(bytes):", data_size)
            isSharded = stats.get("sharded")
            print("Sharded?:", isSharded)
            # Printing Indexes
            if 'idIndex' in cname:
                id_index_value = cname['idIndex']
                print("Index:", id_index_value)
            if isSharded:
                # Check for sharding keys
                collection_info = config_db.collections.find_one({"_id": f"{db_name}.{col_name}"})
                # print(collection_info)
                if not collection_info or not collection_info.get("key"):
                    print("The specified collection is not sharded.")
                else:
                    shard_key = collection_info["key"]
                    print(f"Shard Key: {shard_key}")
                    # Retrieve chunk information from 'config.chunks'
                    chunks = config_db.chunks.find({"ns": f"{db_name}.{col_name}"})
                    # Calculate shard distribution by counting chunks per shard
                    shard_distribution = {}
                    for chunk in chunks:
                        shard_name = chunk["shard"]
                        shard_distribution[shard_name] = shard_distribution.get(shard_name, 0) + 1
                    # Output the shard distribution summary
                    for shard, chunk_count in shard_distribution.items():
                        print(f"Shard: {shard}, Number of Chunks: {chunk_count}")
                        # Retrieve the data size on each shard
                    for shard in shard_distribution.keys():
                        shard_client = MongoClient(client[shard].address)
                        stats = shard_client[db_name].command("collstats", col_name)
                        print(
                            f"Shard: {shard}, Document Count: {stats.get('count')}, Data Size: {stats.get('size')} bytes")
            print("-------")

except Exception as e:
    print(e)


