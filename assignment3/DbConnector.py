from pymongo import MongoClient, version
import os
from dotenv import load_dotenv
import time

load_dotenv()


class DbConnector:
    """
    Connects to the MongoDB server on the Ubuntu virtual machine.
    Connector needs HOST, USER and PASSWORD to connect.

    Example:
    HOST = "tdt4225-00.idi.ntnu.no" // Your server IP address/domain name
    USER = "testuser" // This is the user you created and added privileges for
    PASSWORD = "test123" // The password you set for said user
    """

    def __init__(self,
                 DATABASE='assignment3_mongodb_1',
                 HOST="localhost",
                 PORT="27017",
                 USER="gruppe13",
                 PASSWORD=os.getenv('PASSWORD')):
        uri = "mongodb://%s:%s@%s:%s/" % (USER, PASSWORD, HOST, PORT)

        # Connect to the databases
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        # get database information
        print("You are connected to the database:", self.db.name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # close the cursor
        # close the DB connection
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)

    def create_collection(self, collection_name):
        self.db.create_collection(collection_name)
        print("Collection %s created" % collection_name)

    def remove_collection(self, collection_name):
        if (collection_name in self.db.list_collection_names()):
            self.db.drop_collection(collection_name)
            print("Collection %s removed" % collection_name)

    def create_indexes(self):
        print("creating indexes...")
        self.db["activity"].create_index("user_id")
        self.db["activity"].create_index("transportation_mode")
        self.db["activity"].create_index("start_date_time")
        self.db["trackpoint"].create_index("activity_id")

    def batch_insert_users(self, user_list):
        print("inserting users...")
        start_time = time.time()
        self.db["user"].insert_many(user_list)
        print("finished insert in %s seconds" % (time.time() - start_time))

    def batch_insert_activities_with_id(self, activities):
        print("inserting activities...")
        start_time = time.time()
        try:
            self.db["activity"].insert_many(activities)
            print("finished insert in %s seconds" % (time.time() - start_time))
        except Exception as e:
            print(e)

    def insert_trackpoints_with_id(self, trackpoints):
        # print(trackpoints)
        try:
            self.db["trackpoint"].insert_many(trackpoints)
        except Exception as e:
            print(e)
