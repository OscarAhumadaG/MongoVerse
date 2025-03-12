from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
from datetime import datetime as dt
import pyarrow
from pymongoarrow.api import Schema
from pymongoarrow.monkey import patch_all
import pymongoarrow as pma
from bson import ObjectId


load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://oscar:{password}@cluster0.qfzqr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0&authSource=admin" # we add admin permissions

client = MongoClient(connection_string)


dbs = client.list_database_names()
print(dbs)



production = client.production

def create_book_collection():
    book_validator = {

          "$jsonSchema": {
             "bsonType": "object",
             "required": [ "title", "authors", "publish_date", "type", "copies" ],
             "properties": {
                "title": {
                   "bsonType": "string",
                   "description": "'title' must be a string and is required"
                },
                "authors": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "objectId",
                        "description": "must be and objectId and is required"
                    }
                },
                "publish_date": {
                    "bsonType": "date",
                    "description": "must be a date and is required"
                },
                "type": {
                    "enum": ["Fiction", "Non-Fiction"],
                    "description": "can be only one of the enum values and is required",
                },
                "copies": {
                   "bsonType": "int",
                   "minimum": 0,
                   "description": "must be an integer greater than 0 and is required"
                }
             }
          }
        }


    try:
        production.create_collection("book")
    except Exception as e:
        print(e)

    production.command("collMod", "book", validator= book_validator)

# create_book_collection()

def create_author_collection():
    author_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["first_name", "last_name", "date_of_birth"],
            "properties": {
                "first_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "last_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "date_of_birth": {
                    "bsonType": "date",
                    "description": "must be a date and is required"
                },
            }
        }
    }

    try:
        production.create_collection("author")
    except Exception as e:
        print(e)

    production.command("collMod", "author", validator=author_validator)


# create_author_collection()

def create_data():
    authors = [
        {
            "first_name": "Tim",
            "last_name": "Ruisca",
            "date_of_birth" : dt(2000,7, 20)
        },
        {
            "first_name": "George",
            "last_name": "Orwell",
            "date_of_birth": dt(1903, 6, 25)
        },
        {
            "first_name": "Hernan",
            "last_name": "Melville",
            "date_of_birth": dt(1819, 8, 1)
        },
        {
            "first_name": "F. Scott",
            "last_name": "Fitzgerald",
            "date_of_birth": dt(1896, 9, 24)
        }
    ]

    author_collection = production.author
    authors = author_collection.insert_many(authors).inserted_ids

    books = [
        {"title": "MongoDB Advance Tutorial",
         "authors": [authors[0]],
         "publish_date": dt.today(),
         "type": "Non-Fiction",
         "copies":5
         },
        {"title": "Python for dummies",
         "authors": [authors[0]],
         "publish_date": dt(2022,1, 17),
         "type": "Non-Fiction",
         "copies":5
         },
        {"title": "Nineteen Eighty-Four",
         "authors": [authors[1]],
         "publish_date": dt(1949,6, 8),
         "type": "Fiction",
         "copies":5
         },
        {"title": "The great Gatsy",
         "authors": [authors[3]],
         "publish_date": dt(2014,5, 3),
         "type": "Non-Fiction",
         "copies":5
         },
        {"title": "Moby Dick",
         "authors": [authors[2]],
         "publish_date": dt(1851,9, 24),
         "type": "Non-Fiction",
         "copies": 5
         },
        ]

    book_collection = production.book
    book_collection.insert_many(books)

# create_data()


################## Advanced Queries #################

printer = pprint.PrettyPrinter()

books_containing_a = production.book.find({"title": {"$regex": "a{1}"}})
# printer.pprint(list(books_containing_a))


# Retrieve Authors with Their Corresponding Books
authors_and_books = production.author.aggregate([
    {
        "$lookup": {
            "from": "book",           # The collection to join (books)
            "localField": "_id",      # The field from the "authors" collection
            "foreignField": "authors", # The field in the "book" collection that references authors
            "as": "books"             # The name of the new array field to store the joined results
        }
    }
])


# printer.pprint(list(authors_and_books))

# Count the Number of Books Written by Each Author
authors_books_count = production.author.aggregate([
    {
        "$lookup": {
            "from": "book",           # The collection to join (books)
            "localField": "_id",      # The field from the "authors" collection
            "foreignField": "authors", # The field in the "book" collection that references authors
            "as": "books"             # The name of the new array field to store the joined results
        }
    },
    {
        "$addFields":{
            "total_books": {"$size": "$books"}
        }
    },
    {
        "$project": {"first_name": 1, "last_name": 1, "total_books": 1, "_id":0},
    }
])

# printer.pprint(list(authors_books_count))

# Retrieve Books with Authors Aged Between 50 and 150, Sorted by Age 1st option
books_with_old_authors = production.book.aggregate([
    {
        "$lookup": {
            "from": "author",
            "localField": "authors",
            "foreignField": "_id",
            "as": "authors"
        }
    },
    {
        "$set": {
            "authors": {
                "$map": {
                    "input": "$authors",
                    "in": {
                        "age": {
                            "$dateDiff": {
                                "startDate": "$$this.date_of_birth",
                                "endDate": "$$NOW",
                                "unit": "year"
                            }
                        },
                        "first_name": "$$this.first_name",
                        "last_name": "$$this.last_name",
                    }
                }
            }
        }
    },
    {
        "$match": {
            "$and": [
                {"authors.age": {"$gte": 50}},
                {"authors.age": {"$lte": 150}},
            ]
        }
    },
    {
        "$sort": {
            "age": 1
        }
    }
])

# printer.pprint(list(books_with_old_authors))

# Retrieve Books with Authors Aged Between 50 and 150, Sorted by Age 2nd option
books_with_old_authors2 = production.book.aggregate([
    {
        "$lookup": {
            "from": "author",
            "localField": "authors",
            "foreignField": "_id",
            "as": "authors"
        }
    },
    {
        "$set": {
            "authors": {
                "$map": {
                    "input": "$authors",
                    "in": {
                        "age": {
                            "$dateDiff": {
                                "startDate": "$$this.date_of_birth",
                                "endDate": "$$NOW",
                                "unit": "year"
                            }
                        },
                        "first_name": "$$this.first_name",
                        "last_name": "$$this.last_name"
                    }
                }
            }
        }
    },
    {  # Unwind the "authors" array so that each document has only one author
        "$unwind": "$authors"
    },
    {  # Filter books where at least one author is between 50 and 150 years old
        "$match": {
            "authors.age": {"$gte": 50, "$lte": 150}
        }
    },
    {  # Sort by the author's age
        "$sort": {
            "authors.age": 1
        }
    }
])


# printer.pprint(list(books_with_old_authors2))



patch_all()

# Define a schema for the "author" collection using PyArrow and datetime
author = Schema({
    "_id": ObjectId,             # The unique identifier for each author (MongoDB ObjectId)
    "first_name": pyarrow.string(),  # The author's first name as a string
    "last_name": pyarrow.string(),   # The author's last name as a string
    "date_of_birth": dt              # The author's date of birth
})

# Retrieve all documents from the "author" collection as a Pandas DataFrame using the defined schema
df = production.author.find_pandas_all({}, schema=author)
print(df.head(), "\n\n")  # Print the first few rows of the DataFrame

# Retrieve all documents from the "author" collection as an Apache Arrow Table using the defined schema
arrow_table = production.author.find_arrow_all({}, schema=author)
print(arrow_table, "\n\n")  # Print the Arrow Table

# Retrieve all documents from the "author" collection as NumPy arrays using the defined schema
nd_arrays = production.author.find_numpy_all({}, schema=author)
print(nd_arrays, "\n\n")  # Print the NumPy arrays containing the author data