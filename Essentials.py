from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv, find_dotenv
from bson.objectid import ObjectId
import os
import pprint


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

mongo_db = client.MongoDBcourse
collections = mongo_db.list_collection_names()
print(collections)

def insert_test_doc():
    collection = mongo_db.tutorial
    test_document = {
        "name": "Oscar",
        "type": "Test"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)

# insert_test_doc()

production = client.production
person_collection = production.person_collection

def create_documents():
    first_names = ["Tim", "Sarah", "Jennifer", "Jose", "Brad", "Allen", "Sarah"]
    last_names = ["Ruscica", "Smith", "Bart", "Cater", "Pit", "Geral", "Luna"]
    ages = [21, 40, 23, 19, 34, 67, 33]
    docs = []
    for first_name, last_name, age in zip(first_names, last_names,ages):
        doc = {"first_name": first_name, "last_name": last_name, "age": age}
        docs.append(doc)
        # person_collection.insert_one(doc)
    person_collection.insert_many(docs)


# create_documents()

printer = pprint.PrettyPrinter()
def find_all_people():
    people = person_collection.find({"first_name": "Sarah"}) # pymongo cursor that you can iterate
    # print(list(people)) # This exhausts the cursor!

    for person in people:
        printer.pprint(person)

# find_all_people()

def find_tim():
    tim = person_collection.find_one({"first_name": "Tim"}) # {"first_name": "Tim", "last_name": "Ruscica"}
    printer.pprint(tim)

# find_tim()

def count_all_people():
    count = person_collection.count_documents(filter={}) # within the filter you can add "first_name": "Sarah"
    print(f"Number of people: {count}")

# count_all_people()

def get_person_by_id(person_id):

    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)

# get_person_by_id("67c65f280d6a90556618620b")

def get_age_range(min_age, max_age):
    query = {"$and": [
            {"age": {"$gte": min_age}},
            {"age": {"$lte": max_age}}
        ]}
    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)

# get_age_range(25, 35)

def project_columns():
    """
    You cannot mix 0s and 1s in the same query (except for _id).

    If you include fields (1), you must include all you want and exclude the rest.
    If you exclude fields (0), you must exclude all you don’t want (except _id).
    """
    # columns1 = {"_id": 0, "first_name": 1, "last_name": 1}
    # people = person_collection.find({}, projection=columns1)

    columns = {"_id": 1, "last_name": 1}  # Correct projection (excluding "first_name")
    people = person_collection.find({"first_name": "Sarah"}, projection=columns)

    for person in people:
        printer.pprint(person)

# project_columns()

def update_person_by_id(person_id):

    _id = ObjectId(person_id)

    all_updates = {
        "$set": {"new_field": True},
        "$inc": {"age": 1},
        "$rename": {"first_name": "first", "last_name": "last"}
    }
    person_collection.update_one({"_id": _id},all_updates)

    # person_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})  # unset let you delete a field

# update_person_by_id("67c63c6d4baf000a32db3310")

def replace_one(person_id):
    _id = ObjectId(person_id)

    new_doc = {
        "first_name": "Bryan",
        "last_name": "Ahumada",
        "age": 100
    }

    person_collection.replace_one({"_id": _id}, new_doc)

# replace_one("67c65f280d6a90556618620b")

def delete_doc_by_id(person_id):
    _id = ObjectId(person_id)
    person_collection.delete_one({"_id": _id})
    person_collection.delete_many({})

# delete_doc_by_id("67c65f280d6a90556618620b")

"""
ids_to_delete = ["65a123456789abcd12345678", "65a987654321abcd98765432"]  # Example ObjectIds
object_ids = [ObjectId(id) for id in ids_to_delete]

person_collection.delete_many({"_id": {"$in": object_ids}})

🚨 This will delete all documents in the collection! 🚨
person_collection.delete_many({})
"""


################## Relationship ########################

address ={
    "_id": "6247596410a9126a4cebeb7",
    "street": "Bay Street",
    "number": 2706,
    "city": "San Francisco",
    "country": "United States",
    "zip": "94107"
}

def add_address_embed(person_id, address):
    _id = ObjectId(person_id)
    person_collection.update_one({"_id": _id}, {"$addToSet": {"addresses": address}})

#  add_address_embed("67c63c6d4baf000a32db3310", address)

def add_addres_relatioship(person_id, address):
    _id = ObjectId(person_id)

    address = address.copy()
    address["owner_id"] = person_id

    address_collection = production.address
    address_collection.insert_one(address)


# add_addres_relatioship("67c63c6d4baf000a32db3311", address)
