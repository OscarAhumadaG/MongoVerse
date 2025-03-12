from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv, find_dotenv
from bson.objectid import ObjectId
import os
import pprint
import json


# Load environment variables from a .env file (if it exists)
load_dotenv(find_dotenv())

# Retrieve the MongoDB password from the environment variables
password = os.environ.get("MONGODB_PWD")

# Define the connection URI for the MongoDB cluster using the password from the .env file
# Make sure to create a .env file in your project directory and add the following line:
# MONGODB_PWD=your_mongodb_password
uri = f"mongodb+srv://oscar:{password}@cluster0.qfzqr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

dbs = client.list_database_names()
print(dbs)


jeopardy_db = client.jeopardy_db
question = jeopardy_db.question

printer = pprint.PrettyPrinter()

# Load JSON file (assuming it's in the same directory as this Python script)
with open("JEOPARDY_QUESTIONS1.json", "r", encoding="utf-8") as file:
    questions_data = json.load(file)


# Check if questions already exist in the collection
if question.count_documents({}) == 0:  # If the collection is empty
    result = question.insert_many(questions_data)
    print(f"Inserted {len(result.inserted_ids)} documents into the question collection.")
else:
    print("Questions already exist in the collection. No new insertions.")


def fuzzy_matching():
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search", # search index created in mongo compass
                "text": {
                    "query": "computer",
                    "path": "category",
                    "fuzzy": {}  # Fuzzy matching enabled
                }
            }
        }
        ])
    printer.pprint(list(result)) # Convert cursor to list and print results

fuzzy_matching()

