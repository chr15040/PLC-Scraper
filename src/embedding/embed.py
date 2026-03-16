import argparse
import copy
import csv
import hashlib
import json
import os
import os.path as osp
import time
from collections import defaultdict
from typing import List

from bson import ObjectId
from dotenv import load_dotenv
from langchain_core.documents import Document
from langchain_mongodb.vectorstores import MongoDBAtlasVectorSearch
from langchain_openai import AzureOpenAIEmbeddings
import pandas as pd
from pymongo import MongoClient
import validators

from ..config import *
from ..text_splitter.chunk_page import chunk_page

# s3 = S3Bucket()
# s3.dl_data(scrape_data=True)
load_dotenv()

BATCH_SIZE = 200
DELAY_BETWEEN_BATCHES = 20
PROCESSED_CSV_DIRECTORY = "src/newprocessed"
MONGODB_TIERS = {
    "dev": (MONGODB_URI, INT_RESOURCES_DB_NAME, INT_RESOURCES_COLLECTION),
    # "stg": (STG_MONGODB_URI, INT_RESOURCES_DB_NAME_STG, INT_RESOURCES_COLLECTION),
    # "prd": (PRD_MONGODB_URI, INT_RESOURCES_DB_NAME_PRD, INT_RESOURCES_COLLECTION),
}


def initialize_db_connections():
    connections = {}
    for tier, (uri, db_name, db_coll) in MONGODB_TIERS.items():
        client = MongoClient(uri)
        connections[tier] = client[db_name][db_coll]
    return connections


def delete_documents_by_source(sources):
    """Delete documents from MongoDB collection based on sources."""
    connections = initialize_db_connections()
    for tier, mongo_coll in connections.items():
        try:
            result = mongo_coll.delete_many({"source": {"$in": sources}})
            print(f"{result.deleted_count} documents were deleted from {tier}.")
        except Exception as e:
            print(f"An error occurred while deleting documents: {e}")


def hash_metadata(metadata: dict) -> str:
    """Create a consistent hash of metadata excluding 'chunk_sequence'."""
    metadata_copy = copy.deepcopy(metadata)
    metadata_copy.pop("chunk_sequence", None)
    unique_key = metadata_copy.get("source", "")
    return hashlib.md5(
        (json.dumps(metadata_copy, sort_keys=True) + unique_key).encode()
    ).hexdigest()


def chunk_docs(translate) -> List[Document]:
    docs = []
    sources = []
    df = pd.read_csv(osp.join(PROCESSED_CSV_DIRECTORY, "scraped.csv"))

    grouped = defaultdict(list)
    for _, row in df.iterrows():
        metadata_str = row["metadata"]
        grouped[metadata_str].append(row["content"])

    for metadata_str, content_list in grouped.items():
        try:
            combined_content = " ".join(content_list)
            external_metadata = json.loads(metadata_str)

            if validators.url(external_metadata.get("source", "")):
                sources.append(external_metadata["source"])
            chunks = chunk_page(combined_content, external_metadata, translate)
            if chunks:
                docs.extend(chunks)

        except Exception as e:
            print(f"Failed to process metadata group: {str(e)}")
            with open("failed_chunks.txt", "a", encoding="utf-8-sig") as f:
                f.write(f"{external_metadata['source']} {str(e)}\n")

    return docs, sources


def process_batches(doc_chunks):
    embed = AzureOpenAIEmbeddings(
        model="text-embedding-ada-002",
        azure_endpoint=OPENAI_API_BASE,
        api_key=OPENAI_API_KEY,
    )
    connections = initialize_db_connections()
    inserted_ids_map = defaultdict(dict)
    previous_batch_last_ids = {}

    for i in range(0, len(doc_chunks), BATCH_SIZE):
        batch = doc_chunks[i : i + BATCH_SIZE]

        docs_with_ids = []
        ids = []

        for doc in batch:
            doc_id = ObjectId()
            meta_hash = hash_metadata(doc.metadata)

            chunk_seq = doc.metadata.get("chunk_sequence")
            if chunk_seq is not None:
                inserted_ids_map[meta_hash][chunk_seq] = doc_id

            ids.append(doc_id)
            docs_with_ids.append(doc)

        for tier, mongo_coll in connections.items():
            try:
                MongoDBAtlasVectorSearch.from_documents(
                    docs_with_ids,
                    embed,
                    collection=mongo_coll,
                    index_name=INT_RESOURCES_INDEX,
                    ids=ids,
                )
                print(
                    f"Embedded and inserted {len(docs_with_ids)} documents to {tier}."
                )
            except Exception as e:
                print(f"Error embedding batch: {str(e)}")
                with open("failed_batches.txt", "a", encoding="utf-8-sig") as f:
                    doc_source = set()
                    for doc in docs_with_ids:
                        doc_source.add(doc.metadata.get("source"))
                    for item in doc_source:
                        f.write(f"{item}\n")

            current_batch_map = defaultdict(list)
            for doc, doc_id in zip(batch, ids):
                meta_hash = hash_metadata(doc.metadata)
                chunk_seq = doc.metadata.get("chunk_sequence")

                if chunk_seq is not None:
                    current_batch_map[meta_hash].append((chunk_seq, doc_id))

            for meta_hash, chunks in current_batch_map.items():
                if meta_hash in previous_batch_last_ids:
                    chunks.append(previous_batch_last_ids[meta_hash])

                chunks = sorted(chunks, key=lambda x: x[0])
                for i, (seq, _id) in enumerate(chunks):
                    update_fields = {}
                    if i > 0:
                        update_fields["prev_chunk"] = chunks[i - 1][1]
                    if i < len(chunks) - 1:
                        update_fields["next_chunk"] = chunks[i + 1][1]
                    if update_fields:
                        mongo_coll.update_one({"_id": _id}, {"$set": update_fields})

            time.sleep(DELAY_BETWEEN_BATCHES)

        for meta_hash, (seq, _id) in previous_batch_last_ids.items():
            for tier, mongo_coll in connections.items():
                mongo_coll.update_one({"_id": _id}, {"$unset": {"next_chunk": ""}})


def main():
    parser = argparse.ArgumentParser(description="Process and chunk documents")
    parser.add_argument(
        "--translate", action="store_true", help="Translate documents before chunking"
    )

    args = parser.parse_args()

    doc_chunks, sources = chunk_docs(translate=args.translate)

    if doc_chunks:
        chunk_path = os.path.join(PROCESSED_CSV_DIRECTORY, "chunks.csv")
        with open(chunk_path, "w", newline="", encoding="utf-8-sig") as new_file:
            new_writer = csv.writer(new_file)
            new_writer.writerow(["source", "content"])

            for chunk in doc_chunks:
                new_writer.writerow([chunk.metadata, chunk.page_content])

                print(f"CHUNK: {chunk}\n")
        # if sources:
        #     delete_documents_by_source(sources)
        # process_batches(doc_chunks)


if __name__ == "__main__":
    main()
