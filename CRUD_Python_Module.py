# Example Python Code to Insert a Document

from pymongo import MongoClient, errors
from bson.objectid import ObjectId


class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, username, password):
        # Initializing the MongoClient. This helps to access the MongoDB
        # databases and collections. This is hard-wired to use the aac
        # database, the animals collection, and the aac user.
        #
        # You must edit the password below for your environment.
        #
        # Connection Variables
        #
        USER = username
        PASS = password
        HOST = 'localhost'
        PORT = 27017
        DB = 'aac'
        COL = 'animals'
        #
        # Initialize Connection
        #
        self.client = MongoClient('mongodb://%s:%s@%s:%d' %
                                  (USER, PASS, HOST, PORT))
        self.database = self.client['%s' % (DB)]
        self.collection = self.database['%s' % (COL)]

    # GET the next rec_num for current animals collection
    def nextRecordNum(self):
        aggregateQuery = [
            {"$group": {"_id": None, "rec_num": {"$max": "$rec_num"}}}]

        result = list(self.collection.aggregate(aggregateQuery))

        if result:
            nextRecordNum = result[0]["rec_num"] + 1
            return nextRecordNum
        else:
            raise Exception("Failed to aggregate on maximum rec_num field")

    # CREATE
    # Takes a mongodb document (json object or python typed_dictionary) as its sole parameter
    # Returns True or False depending on whether the record was successfully inserted or the insertion failed, respectively
    # Prints an appropriate message to stdout in each case
    def create(self, data):
        # verify we have a data parameter
        if data is not None:

            # ensure that _id is not defined so that pymongo can handle creation of it
            data.pop("_id", None)

            # iterate and add the newest rec_num to our document before insertion (overwrites any custom rec_num)
            data["rec_num"] = self.nextRecordNum()

            # insert the document
            result = self.database.animals.insert_one(
                data)  # data should be a dictionary

            # verify insertion
            if result.acknowledged is True:
                print("Insert successful with id: %s" % result.inserted_id)
                return True
            else:
                print("Something went wrong when trying to insert the given data")
                return False
        else:
            raise Exception(
                "You must pass in a json or python dictionary to add as an document to the database")

    # READ
    # Takes a classic Mongodb style query (json object or python typed_dictionary) as its sole parameter
    # Returns a list of json objects represented in the matching records in the mongodb instance
    def read(self, query):
        # Verify that we are passed a query
        if query is not None:

            # If we have one attempt to use it and throw an exception if something goes wrong
            try:

                # Utilize the PyMongo find() function with the filter query as a parameter
                cursor = self.collection.find(query)

                # Create a Python list from the cursor results returned by PyMongo find()
                results = list(cursor)

                # Return this list of results to be printed out or used
                return results

            # Catch any malformed MongoDB queries
            except errors.PyMongoError as e:
                print("Error in query:", e)
        else:
            raise Exception("You must pass in a query to the read() function")

    # UPDATE
    # Takes a classic Mongodb style query (json object or python typed_dictionary) and
    # takes another Mongodb style query that represents what key/value pairs should be added
    # to the documents indicated by the prior parameter
    # Returns the number of records updated
    def update(self, filter_query, change_query):

        # Verify that we are given a query for filtering documents and a query for modifying them
        if filter_query and change_query is not None:

            # If we are attempt to find and update the indicated documents
            try:

                # Call PyMongo update_many() with the filter and modification queries
                result = self.collection.update_many(
                    filter_query, change_query)

                # Print the number of records found, and number of records modified
                print("Records matched: %s \nRecords updated: %s \n" %
                      (result.matched_count, result.modified_count))

                # Return the number of records modified / updated
                return result.modified_count

            # Catch any malformed MongoDB queries
            except errors.PyMongoError as e:
                print("Error in query:", e)

        else:
            raise Exception(
                "You must pass in a filterquery and a change query to the update(<filterquery>, <changequery>) function")

    # DELETE
    # Takes a classic Mongodb style query (json object or python typed_dictionary) as its sole parameter
    # Returns the number of documents deleted
    def delete(self, query):

        # Verify that we are given a query parameter
        if query is not None:

            # Attempt to delete with PyMongo delete_many() and our query
            try:

                # store the result of the query to count deletions
                result = self.collection.delete_many(query)

                # If we have deleted anything return the deletion count
                if (result.deleted_count > 0):
                    return result.deleted_count

                # If not return -1 to indicate the query did nothing
                else:
                    return -1

            # Catch any malformed MongoDB queries
            except errors.PyMongoError as e:
                print("Error in query:", e)

        else:
            raise Exception(
                "You must pass in a query to the delete() function")
