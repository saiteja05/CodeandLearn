import pymongo
from pymongo import MongoClient
from pprint import pprint
from collections import defaultdict

# ---------------------------------------------
# 🔧 CONFIGURATION
# ---------------------------------------------
MONGODB_URI = "mongodb+srv://<>:<>@cluster0.tcgzn.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = "ecommerce"

# ---------------------------------------------
# 🚀 Connect to MongoDB
# ---------------------------------------------
client = MongoClient(MONGODB_URI)
db = client[DATABASE_NAME]

# Determine if cluster is shared (M0-M2) or dedicated (M10+)
def is_shared_cluster():
    try:
        cluster_stats = client.admin.command("hostInfo")
        return False  # hostInfo only works on dedicated
    except pymongo.errors.OperationFailure as e:
        return True  # Shared tier doesn't allow hostInfo

shared_cluster = is_shared_cluster()
print(f"\n🔍 Detected Cluster Tier: {'Shared' if shared_cluster else 'Dedicated'}\n")

# ---------------------------------------------
# 📊 Analyze Indexes in Each Collection
# ---------------------------------------------
def analyze_collection_indexes(collection):
    print(f"\n📁 Collection: {collection.name}")
    indexes = list(collection.list_indexes())

    # Collect index usage stats (works only on WiredTiger)
    try:
        index_stats = list(collection.aggregate([{ "$indexStats": {} }]))
        usage_map = {stat['name']: stat['accesses']['ops'] for stat in index_stats}
    except Exception as e:
        print("  ⚠️  Could not fetch index stats. Reason:", str(e))
        usage_map = {}

    # Get index sizes (only on dedicated clusters)
    try:
        stats = db.command("collStats", collection.name)
        index_sizes = stats.get("indexSizes", {})
    except Exception as e:
        print("  ⚠️  Could not fetch index sizes. Reason:", str(e))
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

        # Optimization suggestions
        if name == '_id_':
            continue  # default index
        if ops == 0 and not shared_cluster:
            print("      🚨 Unused index (check for removal if confirmed unused)")
        if len(key) == 1:
            print("      ⚠️  Single-field index — consider compound index if sorted/filtered together")

# ---------------------------------------------
# 🧠 Suggest Index Redundancy & Scope
# ---------------------------------------------
def suggest_index_optimizations():
    print("\n🔧 Global Suggestions:")
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

    if shared_cluster:
        print("\n⚠️ Note: On shared clusters (M0–M2), some commands like `$indexStats`, `hostInfo`, or `collStats` may be limited.")
        print("   → Use query logs and Atlas UI for deep analysis.")
    else:
        print("\n✅ Dedicated cluster — full introspection available.")

if __name__ == "__main__":
    main()