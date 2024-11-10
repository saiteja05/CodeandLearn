from pymongo import MongoClient

# uri = "mongodb+srv://<>:<>@mycluster.mongodb.net/?retryWrites=true&w=majority&appName=StatCollector"
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
            stats = db.command("collstats", col_name)
            data_size = stats.get("size", 0)
            print("size(bytes):", data_size)
            print(f"Document count: {stats['count']}")
            # Printing Indexes
            if 'idIndex' in cname:
                id_index_value = cname['idIndex']
                print("Index:", id_index_value)
                print(f"Index size: {stats['totalIndexSize']} bytes")
                for index_name, size in stats['indexSizes'].items():
                    print(f" - {index_name}: {size} bytes")

            # Check sfor sharding keys
            collection_info = config_db.collections.find_one({"_id": f"{db_name}.{col_name}"})
            if not collection_info or not collection_info.get("key"):
                print("The specified collection is not sharded.")
            else:
                shard_key = collection_info["key"]
                print(f"Shard Key: {shard_key}")
                # Retrieve chunk information from 'config.collections' and 'config.chunks'
                shard_distribution = {}
                for col in config_db.collections.find({"_id": f"{db_name}.{col_name}"}):
                    # Calculate shard distribution by counting chunks per shard
                     for chunk in config_db.chunks.find({"uuid": col['uuid']}):
                        shard_name = chunk["shard"]
                        shard_distribution[shard_name] = shard_distribution.get(shard_name, 0) + 1
                    # Output the shard distribution summary
                     for shard, chunk_count in shard_distribution.items():
                        print(f"Shard: {shard}, Number of Chunks: {chunk_count}")

                    # chunks = config_db.chunks.find({"_id": f"{db_name}.{col_name}"})
                    # #Calculate shard distribution by counting chunks per shard
                    # shard_distribution = {}
                    # for chunk in chunks:
                    #     shard_name = chunk["shard"]
                    #     shard_distribution[shard_name] = shard_distribution.get(shard_name, 0) + 1
                    #  # Output the shard distribution summary
                    # for shard, chunk_count in shard_distribution.items():
                    #     print(f"Shard: {shard}, Number of Chunks: {chunk_count}")
                    #     #Retrieve the data size on each shard
                    # for shard in shard_distribution.keys():
                    #     shard_client = MongoClient(client[shard].address)
                    #     stats = shard_client[db_name].command("collstats", col_name)
                    #     print(f"Shard: {shard}, Document Count: {stats.get('count')}, Data Size: {stats.get('size')} bytes")
            print("-------")

except Exception as e:
    print(e)


