import subprocess
import json
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymongo
import shlex

# ## CONFIGURATION ##
# ##############################################################################

# Enable debug mode (if True, prints commands and queries to the console)
# WARNING: In debug mode, the MongoDB URI (including username and password) may be exposed in the console.
DEBUG_MODE = True

# Maximum number of concurrent jobs
MAX_CONCURRENT_JOBS = 4

# MongoDB connection URIs
SOURCE_MONGO_URI = "mongodb://localhost:30000,localhost:30001,localhost:30002/?replicaSet=rs0"
TARGET_MONGO_URI = "mongodb://localhost:20000,localhost:20001,localhost:20002/?replicaSet=target"

# Default source and target database names
SOURCE_DB = "test"
TARGET_DB = "test"

# Collection name to record migration progress
LOG_COLLECTION_NAME = "migration_logs"

# List of collections to migrate
# Each item is a dictionary with the following keys:
# - 'name': Source collection name (required)
# - 'source_db': Source DB name (optional, uses global SOURCE_DB if not specified)
# - 'target_db': Target DB name (optional, uses global TARGET_DB if not specified)
# - 'target_name': Target collection name (optional, same as 'name' if not specified)
# - 'query': JSON query string to be passed to the mongodump -q option (optional)
COLLECTIONS_TO_MIGRATE = [
    {
        "name": "dk1",
        "target_db": "target_test",
        "query": '{"fld1": {"$gt": {"$date": "2018-02-15T15:55:00.176Z"}}}'
    },
    {
        "name": "dk2",
        "target_db": "target_test",
        "query": '{"fld4": "magna aliquyam erat, sed diam"}'
    },
    {
        "name": "dk3",
        "target_db": "target_test",
        "query": '{"fld6": 437549}'
    },
    {
        "name": "dk4",
        "target_db": "target_test"
    },
    {
        "source_db": "test1",
        "name": "dk1",
        "target_db": "target_test",
        "target_name": "test1_dk1"
    },
    {
        "source_db": "test1",
        "name": "dk2",
        "target_db": "target_test",
        "target_name": "test1_dk2"
    },
    {
        "source_db": "test1",
        "name": "dk3",
        "target_db": "target_test",
        "target_name": "test1_dk3"
    },
    {
        "source_db": "test1",
        "name": "dk4",
        "target_db": "target_test",
        "target_name": "test1_dk4"
    },
    {
        "source_db": "test2",
        "name": "dk1",
        "target_db": "target_test",
        "target_name": "test2_dk1"
    },
    {
        "source_db": "test2",
        "name": "dk2",
        "target_db": "target_test",
        "target_name": "test2_dk2"
    },
    {
        "source_db": "test2",
        "name": "dk3",
        "target_db": "target_test",
        "target_name": "test2_dk3"
    },
    {
        "source_db": "test2",
        "name": "dk4",
        "target_db": "target_test",
        "target_name": "test2_dk4"
    },
    # Non-existent collection for failure testing
    {
        "name": "non_existent_collection"
    }
]

# ##############################################################################


def convert_extended_json_to_native(obj):
    """
    Converts an Extended JSON object (e.g., {"$date": ...}) to a native Python type that Pymongo recognizes.
    """
    if isinstance(obj, dict):
        # Convert date object
        if "$date" in obj and len(obj) == 1:
            dt_str = obj["$date"]
            # Remove 'Z' and add timezone info (Python 3.11+ recognizes 'Z' automatically)
            if dt_str.endswith('Z'):
                dt_str = dt_str[:-1] + '+00:00'
            return datetime.fromisoformat(dt_str)

        # Other Extended JSON types (like ObjectId) can be added here if needed

        new_dict = {}
        for key, value in obj.items():
            new_dict[key] = convert_extended_json_to_native(value)
        return new_dict

    if isinstance(obj, list):
        return [convert_extended_json_to_native(item) for item in obj]

    return obj

def get_db_connection(uri):
    """Returns a MongoDB client connection."""
    try:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    except pymongo.errors.ConnectionFailure as e:
        print(f"Error: Could not connect to MongoDB at {uri}. {e}")
        return None

def update_migration_log(log_collection, log_data):
    """Updates the migration log in MongoDB."""
    try:
        log_collection.update_one(
            {"source_db": log_data["source_db"], "source_collection": log_data["source_collection"]},
            {"$set": log_data},
            upsert=True
        )
    except Exception as e:
        print(f"Error updating migration log for {log_data['source_db']}.{log_data['source_collection']}: {e}")


def migrate_collection(config):
    """Migrates a single collection using a mongodump and mongorestore pipeline."""
    source_db_name = config.get('source_db', SOURCE_DB)
    target_db_name = config.get('target_db', TARGET_DB)
    source_collection_name = config['name']
    target_collection_name = config.get('target_name', source_collection_name)
    query = config.get('query')

    start_time = datetime.utcnow()
    log_data = {
        "source_db": source_db_name, "source_collection": source_collection_name,
        "target_db": target_db_name, "target_collection": target_collection_name,
        "query": query, "start_time": start_time, "status": "running",
    }

    target_client_for_log = None
    try:
        target_client_for_log = get_db_connection(TARGET_MONGO_URI)
        if not target_client_for_log:
            raise ConnectionError("Failed to connect to target DB for logging.")
        log_collection = target_client_for_log[TARGET_DB][LOG_COLLECTION_NAME]
        update_migration_log(log_collection, log_data)
    except Exception as e:
        print(f"Initial logging failed for {source_db_name}.{source_collection_name}: {e}")
        return {"status": "failed", "collection": f"{source_db_name}.{source_collection_name}", "error": str(e)}

    print(f"Starting migration for: {source_db_name}.{source_collection_name} -> {target_db_name}.{target_collection_name}")

    try:
        dump_cmd = [
            'mongodump', '--uri', SOURCE_MONGO_URI, '--db', source_db_name,
            '--collection', source_collection_name, '--archive'
        ]
        if query:
            dump_cmd.extend(['-q', query])

        restore_cmd = [
            'mongorestore', '--uri', TARGET_MONGO_URI, '--archive', '--drop',
            f'--nsFrom={source_db_name}.{source_collection_name}',
            f'--nsTo={target_db_name}.{target_collection_name}'
        ]

        if DEBUG_MODE:
            print(f"  [DEBUG] mongodump command: {shlex.join(dump_cmd)}")
            print(f"  [DEBUG] mongorestore command: {shlex.join(restore_cmd)}")

        dump_proc = subprocess.Popen(dump_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        restore_proc = subprocess.Popen(restore_cmd, stdin=dump_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        dump_proc.stdout.close()
        restore_stdout, restore_stderr = restore_proc.communicate()
        dump_stdout, dump_stderr = dump_proc.communicate()

        if restore_proc.returncode != 0:
            raise Exception(f"Restore failed. Stderr: {restore_stderr.decode('utf-8', 'ignore')}")
        if dump_proc.returncode != 0:
            raise Exception(f"Dump failed. Stderr: {dump_stderr.decode('utf-8', 'ignore')}")

        source_client = get_db_connection(SOURCE_MONGO_URI)
        target_client = get_db_connection(TARGET_MONGO_URI)

        query_filter_for_count = {}
        if query:
            extended_json_dict = json.loads(query)
            query_filter_for_count = convert_extended_json_to_native(extended_json_dict)

        if DEBUG_MODE:
            print(f"  [DEBUG] Source count Pymongo filter: {str(query_filter_for_count)}")
            print(f"  [DEBUG] Target count Pymongo filter: {{}}")

        source_count = source_client[source_db_name][source_collection_name].count_documents(query_filter_for_count)
        target_count = target_client[target_db_name][target_collection_name].count_documents({})

        verification_status = "success"
        if source_count != target_count:
            verification_status = "count_mismatch"
            print(f"WARNING: Count mismatch for {source_db_name}.{source_collection_name}: Source({source_count}) != Target({target_count})")
        else:
            print(f"Verification successful for {source_db_name}.{source_collection_name} (Count: {source_count})")

        log_data.update({
            "status": "completed", "end_time": datetime.utcnow(),
            "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
            "source_count": source_count, "target_count": target_count,
            "verification": verification_status
        })
        update_migration_log(log_collection, log_data)

        if source_client: source_client.close()
        if target_client: target_client.close()

        return {"status": "completed", "collection": f"{source_db_name}.{source_collection_name}", "verification": verification_status}

    except Exception as e:
        error_msg = str(e)
        print(f"Migration FAILED for {source_db_name}.{source_collection_name}. Reason: {error_msg}")
        log_data.update({
            "status": "failed", "end_time": datetime.utcnow(),
            "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
            "error_message": error_msg
        })
        update_migration_log(log_collection, log_data)

        return {"status": "failed", "collection": f"{source_db_name}.{source_collection_name}", "error": error_msg}
    finally:
        if target_client_for_log:
            target_client_for_log.close()


def main():
    """Main function to coordinate migration tasks."""
    start_time = datetime.now()
    print(f"Migration process started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    if DEBUG_MODE:
        print("--- DEBUG MODE IS ENABLED ---")
    print("-" * 50)

    successful_migrations = []
    failed_migrations = []

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS) as executor:
        future_to_collection = {executor.submit(migrate_collection, config): config for config in COLLECTIONS_TO_MIGRATE}

        for future in as_completed(future_to_collection):
            config = future_to_collection[future]
            collection_id = f"{config.get('source_db', SOURCE_DB)}.{config['name']}"
            try:
                result = future.result()
                if result['status'] == 'completed':
                    successful_migrations.append(result)
                else:
                    failed_migrations.append(result)
            except Exception as exc:
                print(f"An exception occurred during migration of {collection_id}: {exc}")
                failed_migrations.append({"status": "failed", "collection": collection_id, "error": str(exc)})

    end_time = datetime.now()
    print("-" * 50)
    print(f"Migration process finished at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total duration: {(end_time - start_time).total_seconds():.2f} seconds")
    print("\n--- Migration Summary ---")

    print(f"\nSuccessful migrations ({len(successful_migrations)}):")
    for r in successful_migrations:
        print(f"  - {r['collection']} (Verification: {r.get('verification', 'N/A')})")

    if failed_migrations:
        print(f"\nFailed migrations ({len(failed_migrations)}):")
        for r in failed_migrations:
            print(f"  - {r['collection']}")
    else:
        print("\nAll migrations completed successfully!")


if __name__ == "__main__":
    main()
