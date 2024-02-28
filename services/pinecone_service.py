import csv
import json
import os
from pinecone import Pinecone, ServerlessSpec

class PineconeService:
    def __init__(self, logger=None):
        self.api_key = os.getenv("PINECONE_API_KEY")
        self.logger = logger
        self.pc = None
        self.index = None
        self.initialized = False

    def init_pinecone(self, index_name):
        if self.initialized:
            self.logger.info("Pinecone is already initialized. Reinitializing...")
        try:
            self.logger.info("Initializing Pinecone...")
            self.pc = Pinecone(api_key=self.api_key)
            self.index = self.pc.Index(index_name)
            stats = self.index.describe_index_stats()
            if stats:
                self.logger.info(f"Pinecone initialized successfully. Stats: {stats}")
                self.initialized = True
            else:
                self.logger.error("Failed to retrieve index stats, Pinecone initialization may have failed.")
        except Exception as e:
            self.logger.error(f"Pinecone initialization failed: {str(e)}")
            raise e

    def upsert_vectors(self, vectors):
        try:
            self.logger.info(f"Found {len(vectors)} vectors in the JSON file.")
            # Transform vectors into the correct format
            formatted_vectors = [(vec["id"], vec["values"], vec.get("metadata", {})) for vec in vectors]

            # Batch vectors in sets of 100
            self.logger.info("Batching vectors...")
            vector_batches = [formatted_vectors[i:i + 100] for i in range(0, len(formatted_vectors), 100)]

            for i, batch in enumerate(vector_batches):
                self.logger.info(f"Upserting batch {i + 1}...")
                self.index.upsert(vectors=batch)  # Make sure this matches your Pinecone SDK's method signature

            self.logger.info("Vectors successfully upserted to Pinecone.")
        except Exception as e:
            self.logger.error(f"An error occurred during Pinecone upsert: {str(e)}")
            raise e

    def fetch_all_vectors_and_metadata(self, batch_size=100):
        all_data = []

        try:
            # Describe the index to get the total number of vectors
            stats = self.index.describe_index_stats()
            total_vectors = stats["namespaces"][self.namespace]["vector_count"]

            all_ids = [str(i) for i in range(total_vectors)]
            self.logger.info(f"Total vectors: {total_vectors}")

            # Batch function to split all_ids into smaller chunks
            def batch(iterable, n=1):
                l = len(iterable)
                for ndx in range(0, l, n):
                    yield iterable[ndx : min(ndx + n, l)]

            # Fetch vectors and metadata
            for id_batch in batch(all_ids, batch_size):
                self.logger.info(f"Fetching batch {id_batch} of size {batch_size}...")
                fetch_response = self.index.fetch(ids=id_batch)

                if fetch_response is None:
                    self.logger.warning(f"No data returned for batch {id_batch}.")
                    continue

                if "vectors" in fetch_response:
                    all_data.extend(fetch_response["vectors"].values())
                else:
                    self.logger.warning(
                        f"Unexpected response format for batch {id_batch}."
                    )

            self.save_to_csv(all_data)

            return all_data

        except Exception as e:
            self.logger.error(f"An error occurred during Pinecone fetch: {str(e)}")
        return all_data

    def save_to_csv(self, data):
        try:
            with open("pinecone_data.csv", mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["ID", "Values", "Metadata"])

                for d in data:
                    id_ = d.get("id", "")
                    values = d.get("values", [])
                    metadata = d.get("metadata", {})

                    writer.writerow(
                        [id_, ",".join(map(str, values)), json.dumps(metadata)]
                    )

            print("Data saved to CSV file successfully.")
        except Exception as e:
            print(f"An error occurred while saving to CSV: {str(e)}")

    def delete_vectors(self, ids):
        try:
            self.logger.info(f"Deleting vectors with IDs: {ids}...")
            self.index.delete(ids=ids)
            self.logger.info("Vectors successfully deleted.")
        except Exception as e:
            self.logger.error(f"An error occurred during Pinecone delete: {str(e)}")
            raise e

    def update_vector(self, id, values, metadata=None):
        try:
            self.logger.info(f"Updating vector with ID: {id}...")
            self.index.upsert(vectors=[(id, values, metadata or {})])
            self.logger.info("Vector successfully updated.")
        except Exception as e:
            self.logger.error(f"An error occurred during Pinecone update: {str(e)}")
            raise e
