from pymongo import MongoClient, UpdateOne
import time


def add_offset_counter(username, password, host, database_name, collection_name, batch_size=1000):
    """
    Add an incremental offset counter starting from 0 to a MongoDB collection.

    Args:
        username (str): MongoDB username
        password (str): MongoDB password
        host (str): MongoDB host address
        database_name (str): Name of the database
        collection_name (str): Name of the collection
        batch_size (int): Number of documents to process in each batch
    """
    # Construct connection string with authentication
    connection_string = f"mongodb://{username}:{password}@{host}"

    # Connect to MongoDB
    client = MongoClient(connection_string)
    db = client[database_name]
    collection = db[collection_name]

    # Get total count of documents
    total_docs = collection.count_documents({})
    print(f"Total documents to process: {total_docs:,}")

    start_time = time.time()
    processed = 0
    offset_counter = 0

    try:
        # Drop existing offset index if it exists
        collection.drop_index("offset_1")
        print("Dropped existing offset index")
    except:
        print("No existing offset index to drop")

    try:
        # Process documents in batches
        cursor = collection.find({}).batch_size(batch_size)

        while True:
            batch = []

            # Prepare batch of updates
            for _ in range(batch_size):
                try:
                    doc = next(cursor)
                    batch.append(UpdateOne(
                        {"_id": doc["_id"]},
                        {"$set": {"offset": offset_counter}}
                    ))
                    offset_counter += 1
                except StopIteration:
                    break

            if not batch:
                break

            # Execute batch update
            collection.bulk_write(batch, ordered=False)
            processed += len(batch)

            # Calculate and display progress
            elapsed_time = time.time() - start_time
            progress = (processed / total_docs) * 100
            rate = processed / elapsed_time if elapsed_time > 0 else 0

            print(f"Progress: {progress:.1f}% ({processed:,}/{total_docs:,})")
            print(f"Processing rate: {rate:.0f} documents/second")
            print(f"Estimated time remaining: {((total_docs - processed) / rate) / 60:.1f} minutes")
            print("-" * 50)

        # Create the unique index after all documents are updated
        print("Creating unique index on offset field...")
        collection.create_index("offset", unique=True)
        print("Index created successfully")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        client.close()

    total_time = time.time() - start_time
    print(f"\nProcess completed in {total_time / 60:.1f} minutes")
    print(f"Total documents processed: {processed:,}")


# Example usage
if __name__ == "__main__":
    # Replace these with your actual MongoDB connection details
    USERNAME = "utube"
    PASSWORD = "utube"
    HOST = "mongodb:27017"
    DATABASE_NAME = "utube"
    COLLECTION_NAME = "videos"
    BATCH_SIZE = 1000  # Adjust based on your system's capabilities

    add_offset_counter(
        USERNAME,
        PASSWORD,
        HOST,
        DATABASE_NAME,
        COLLECTION_NAME,
        BATCH_SIZE
    )
