import datetime
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import time
import pymongo
import haversine
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
        self.db["trackpoint"].create_index([("location", pymongo.GEOSPHERE)])

    def batch_insert_users(self, user_list):
        print("inserting users...")
        start_time = time.time()
        try:
            self.db["user"].insert_many(user_list)
            print("finished insert in %s seconds" % (time.time() - start_time))
        except Exception as e:
            print(e)

    def batch_insert_activities_with_id(self, activities):
        print("inserting activities...")
        start_time = time.time()
        try:
            self.db["activity"].insert_many(activities)
            print("finished insert in %s seconds" % (time.time() - start_time))
        except Exception as e:
            print(e)

    def insert_trackpoints_with_id(self, trackpoints):
        try:
            self.db["trackpoint"].insert_many(trackpoints)
        except Exception as e:
            print(e)

    # 2

    def find_average_activities_per_user(self):
        return self.db["activity"].aggregate([
            {
                "$group": {
                    "_id": "$user_id",
                    "count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": "null",
                    "avg": {"$avg": "$count"}
                }
            }
        ])

    # 3

    def find_top_20_users_with_most_activities(self):
        return self.db["activity"].aggregate([
            {
                "$group": {
                    "_id": "$user_id",
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"count": -1}
            },
            {
                "$limit": 20
            }
        ])

    # 4

    def find_users_who_have_taken_taxi(self):
        return self.db["activity"].aggregate([
            {
                "$match": {
                    "transportation_mode": "taxi"
                }
            },
            {
                "$group": {
                    "_id": "$user_id"
                }
            }
        ])

    # 5

    def find_all_types_of_transportation_modes(self):
        return self.db["activity"].aggregate([
            {
                "$match": {
                    "transportation_mode": {"$ne": False}
                }
            },
            {
                "$group": {
                    "_id": "$transportation_mode",
                    "count": {"$sum": 1}
                }
            }
        ])

    # 6 a

    def find_year_with_most_activities(self):
        return self.db["activity"].aggregate([
            {
                "$group": {
                    "_id": {"$year": "$start_date_time"},
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"count": -1}
            },
            {
                "$limit": 1
            }
        ])

    # 6 b

    # TODO Need to validate result. Hours seems a bit off
    def find_year_with_most_recorded_hours(self):
        return self.db["activity"].aggregate([
            {
                "$group": {
                    "_id": {"$year": "$start_date_time"},
                    "count": {"$sum": {"$subtract": ["$end_date_time",
                                                     "$start_date_time"]}}
                }
            },
            {
                "$sort": {"count": -1}
            },
            {
                "$limit": 1
            }
        ])

    # 7
    # Find all trackpoint of a given activity_id
    def find_trackpoints_of_activity(self, activity_id):
        return self.db["trackpoint"].find({"activity_id": activity_id})

    # Find all activty ids where transportation mode is "walk" in 2008 for user with user_id "112"
    def find_all_activity_ids_for_user_112_in_2008(self):
        return self.db["activity"].aggregate([
            {
                "$match": {
                    "user_id": "112",
                    "transportation_mode": "walk",
                    # "start_date_time": {"$regex": "2008"}
                    "start_date_time": {"$gte": datetime(2008, 1, 1)},
                    "end_date_time": {"$lt": datetime(2009, 1, 1)}
                }
            },
            {
                "$project": {
                    "activity_id": 1
                }
            }
        ])

    # Todo 8

    # Return a list of all user_ids

    def find_all_user_ids(self):
        return self.db["user"].find({}, {"user_id": 1})

    # Find all activity_ids of a given user_id
    def find_all_activity_ids_of_user(self, user_id):
        return self.db["activity"].aggregate([
            {
                "$match": {
                    "user_id": user_id,
                }
            },
            {
                "$project": {
                    "activity_id": 1
                }
            }])

    # Find the altitude of each trackpoint of a given activity_id
    def find_altitude_of_activity(self, activity_id):
        return self.db["trackpoint"].find({"activity_id": activity_id}, {"altitude": 1})

    # 9

    #  Find the all the date_time values of each trackpoint of a given activity_id
    def find_date_time_of_activity(self, activity_id):
        return self.db["trackpoint"].find({"activity_id": activity_id}, {"date_time": 1})

    # Todo - 10

    def find_users_with_activity_with_trackpoint_at_location(self):
        results = self.db["trackpoint"].aggregate([
            {
                '$geoNear': {
                    'near': {
                        'type': 'Point',
                        'coordinates': [
                            116.397, 39.916
                        ]
                    },
                    'distanceField': 'dist.calculated',
                    'maxDistance': 100,
                    'includeLocs': 'dist.location',
                    'spherical': True
                }
            }])
        activity_ids = []
        for result in results:
            activity_ids.append(result["activity_id"])

        for user in self.db["activity"].aggregate([
            {
                "$match": {
                    "_id": {"$in": activity_ids}
                }
            },
            {
                "$group": {
                    "_id": "$user_id"
                }
            }
        ]):
            print(user)

    # TODO - 11 Why is first (userID=10) not correct. Should be 'taxi', but is 'bus'

    def find_all_users_who_have_registered_transportation_mode(self):
        return self.db["activity"].aggregate([
            {
                "$match": {
                    "transportation_mode": {"$ne": False}
                }
            },
            {
                "$group": {
                    "_id": "$user_id",
                    "transportation_mode": {"$first": "$transportation_mode"}
                }
            },
            {
                "$sort": {"_id": 1}
            }
        ])
