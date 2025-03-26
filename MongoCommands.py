####### SERVICES #########
# To activate MongoDB
"""
net start MongoDB
"""

# To Deactivate MongoDB
"""
net stop MongoDB
sc stop MongoDB
"""

# To disable completely MongoDB
"""
sc config MongoDB start= disabled
"""

# To enable completely MongoDB
"""
sc config MongoDB start= auto
"""

# To see if Mongo service is running
"""
sc query MongoDB
"""

# To open Mongo Shell
"""
mongosh
"""

####### DATABASES #########

# Inside the Mongo Shell you can check your databases
"""
show dbs;
"""

# To select one of the DB or you create a new db by using the same command
"""
use local;

use pruebas; 
"""

# Drop the Database
"""
db.dropDatabase()
"""

####### COLLECTIONS #########

# To show collections
"""
show collections
"""

####### INSERT DOCUMENTS #########
# To create a collection and add a document at the same time
# First you create the document
"""
document = {
  "name": "Oscar Dario",
  "lastname": "Ahumada Gomez",
  "date_of_birth": "26-09-1988",
  "age": 35,
  "balance": 987.432,
  "hobbies": ["music", "football", "reading"],
  "orders": [],
  "creditors": null
}
"""

# Then you insert the document in the new collection the collection will be created by MongoDB dynamically
"""
db.people_collection.insert(document)
"""


# To insert just one document you can use also
"""
db.collection_name.insertOne({
    "name": "Oscar Dario",
    "lastname": "Ahumada Gomez",
    "age": 35
});
"""

# To insert just more than one document
"""
db.collection_name.insertMany([
    {"name": "Sarah", "lastname": "Smith", "age": 40},
    {"name": "Jose", "lastname": "Cater", "age": 19}
]);
"""

####### FIND DOCUMENTS INTO A COLLECTION #########

# To find documents in the collection
"""
db.name_of_collection.find()
"""

# To visualize better the information you can use the function pretty()
"""
db.collection_name.find().pretty()
"""


####### UPDATE DOCUMENTS #########

# Adding a New Field to a Document
"""
db.name_of_collection.update(
  { "first_name": "Oscar" },  
  { $set: { "nationality": "Colombian" } }  
)
# Find the document where first_name is "Oscar"
# Add the new field "nationality"
"""

# To update all the documents by adding a new field
"""
db.name_of_collection.updateMany(
  {},
  { $set: { "status": "pending" } }
)

# Adding the new field "status" in all the documents
"""


# to update a document in a database
"""
db.name_of_collection.update(
  { "first_name": "Oscar" },  
  { $set: { "age": 36,
  "city": "Montreal"}},
  { upsert: false })
  
# Criteria: Find the document with first_name "Oscar"
# Update age to 36
# Update city to Montreal
# Do not insert a new document if no match is found
"""


# To update multiple documents in the collection
"""
db.name_of_collection.update(
  { "age": { $lt: 30 } },      
  { $set: { "status": "young" } }, 
  { multi: true, upsert: false }  
)

 # Criteria: find documents with age less than 30
 # Update operation: set the status field to "young"
 # Update all matching documents, no insertion if not found
"""

# To insert a document if no matching document is found
"""
db.name_of_collection.update(
  { "first_name": "John" },     
  { $set: { "last_name": "Doe", "age": 28 } }, 
  { upsert: true }              
)

# Criteria: find the document with first_name "John"
# Update operation: set last_name and age
# If no matching document is found, insert a new one
"""


# To push a new hobby to the hobbies array
"""
db.name_of_collection.update(
  { "first_name": "Oscar" },   
  { $push: { "hobbies": "coding" } },  
  { upsert: false }            
)

# Criteria: find the document with first_name "Oscar"
# Update operation: add "coding" to the hobbies array
# Do not insert a new document if no match is found
"""


# To remove a field from a document
"""
db.name_of_collection.update(
  { "first_name": "Oscar" },   
  { $unset: { "middle_name": "" } },  
  { upsert: false }            
)
# Criteria: find the document with first_name "Oscar"
# Update operation: remove the middle_name field
# No insertion if the document doesn't exist
"""


# To update a nested field within an embedded document
"""
db.name_of_collection.update(
  { "address.city": "Montreal" },  
  { $set: { "address.zip": "H3Z 2Y7" } },  
  { upsert: false }             
)

# Criteria: find the document where the city is Montreal
# Update operation: change the zip code
# No insertion if the document doesn't exist
"""


####### DELETE DOCUMENTS #########

# Delete a Single Document
"""
db.name_of_collection.deleteOne({ "first_name": "Oscar" })

"""

# Delete All Documents Matching a Condition
"""
db.name_of_collection.deleteMany({ "status": "inactive" })

# Removes all documents where status is inactive
"""

# Remove All Documents in a Collection, if you just want to remove data but keep the collection
"""
db.name_of_collection.deleteMany({})
"""

# Dropping a Collection
"""
db.name_of_collection.drop()
"""


# You can create a collection with schema validation using the following command
"""
db.createCollection("users", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["name", "email", "age"],
      properties: {
        name: {
          bsonType: "string",
          description: "Must be a string and is required"
        },
        email: {
          bsonType: "string",
          pattern: "^[^@]+@[^@]+\\.[^@]+$",
          description: "Must be a valid email format"
        },
        age: {
          bsonType: "int",
          minimum: 18,
          maximum: 100,
          description: "Must be an integer between 18 and 100"
        },
        address: {
          bsonType: "object",
          properties: {
            street: { bsonType: "string" },
            city: { bsonType: "string" }
          }
        }
      }
    }
  }
});
"""


# To update the schema using collMod
"""
db.runCommand({
  collMod: "users",
  validator: { /* New schema */ }
});
"""

