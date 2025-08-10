import pymongo
from pymongo import MongoClient
from pprint import pprint
# from collections import defaultdict

# ---------------------------------------------
# 🔧 CONFIGURATION
# ---------------------------------------------
MONGODB_URI = "mongodb+srv://locust:locust@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = "ecommerce"

# ---------------------------------------------
# 🚀 Connect to MongoDB
# ---------------------------------------------
# ---------------------------------------------
# 🚀 Connect to MongoDB
# ---------------------------------------------
client = MongoClient(MONGODB_URI)
db = client[DATABASE_NAME]

def detect_deployment_type():
    try:
        build_info = client.admin.command("buildInfo")
        print(build_info)
        atlas_version = build_info.get("atlasVersion", None)
        modules = build_info.get("modules", [])
        version = build_info.get("version", "unknown")

        # Enterprise on-prem
        if "enterprise" in modules:
            return f"MongoDB Enterprise (version {version})"

        return f"MongoDB Community (version {version})"

    except Exception as e:
        return f"⚠️ Unknown Deployment (error: {str(e)})"

def is_shared_cluster():
    try:
        client.admin.command("hostInfo")
        return False
    except pymongo.errors.OperationFailure as e:
        print("⚠️ Could not execute hostInfo (likely restricted privileges)")
        return False  # Assume dedicated unless proven otherwise

shared_cluster = is_shared_cluster()
print("🧠 MongoDB Deployment:", detect_deployment_type())
# ---------------------------------------------
# 📊 Analyze Indexes in Each Collection
# ---------------------------------------------
def analyze_collection_indexes(collection):
    print(f"\n📁 Collection: {collection.name}")
    indexes = list(collection.list_indexes())

    # Collect index usage stats
    try:
        index_stats = list(collection.aggregate([{ "$indexStats": {} }]))
        usage_map = {stat['name']: stat['accesses']['ops'] for stat in index_stats}
    except Exception as e:
        print(f"⚠️ Could not fetch indexStats for {collection.name}: {str(e)}")
        usage_map = {}

    # Get index sizes
    try:
        stats = db.command("collStats", collection.name)
        index_sizes = stats.get("indexSizes", {})
    except Exception as e:
        print(f"⚠️ Could not fetch collStats for {collection.name}: {str(e)}")
        index_sizes = {}

    for idx in indexes:
        name = idx['name']
        key = idx['key']
        ops = usage_map.get(name, 0)
        size = index_sizes.get(name, "N/A")

        print(f"   ➤ Index: {name}")
        print(f"      Key: {dict(key)}")
        print(f"      Usage Ops: {ops}")
        print(f"      Size: {size if isinstance(size, str) else str(round(size / 1024, 2)) + ' KB'}")

        if name == '_id_':
            continue  # skip default index
        if ops == 0 and usage_map:
            print("      🚨 Unused index — candidate for review")
        if len(key) == 1:
            print("      ⚠️  Single-field index — consider compound if part of multi-filter/sort queries")

# ---------------------------------------------
# 🧠 Suggest Index Redundancy & Scope
# ---------------------------------------------
def suggest_index_optimizations():
    print("\n🔧 Global Index Suggestions:")
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        try:
            index_list = list(collection.list_indexes())
        except Exception:
            continue

        seen_keys = set()
        for idx in index_list:
            key_tuple = tuple(idx['key'].items())
            for seen in seen_keys:
                if key_tuple[:len(seen)] == seen:
                    print(f"   🔁 Index in {collection.name} ({key_tuple}) is a prefix of {seen} — possible redundancy")
            seen_keys.add(key_tuple)

# ---------------------------------------------
# 🧪 Main Execution
# ---------------------------------------------
def main():
    print("📦 MongoDB Index Analyzer\n")
    for coll_name in db.list_collection_names():
        collection = db[coll_name]
        try:
            analyze_collection_indexes(collection)
        except Exception as e:
            print(f"   ❌ Skipping {coll_name} due to error: {str(e)}")

    suggest_index_optimizations()

    print("\n✅ Index inspection complete.")

if __name__ == "__main__":
    main()