import mysql.connector as mysql
# from decouple import config
import os
from dotenv import load_dotenv
import time
from haversine import haversine, Unit

load_dotenv()


class DbConnector:
    """
    Connects to the MySQL server on the Ubuntu virtual machine.
    Connector needs HOST, DATABASE, USER and PASSWORD to connect,
    while PORT is optional and should be 3306.

    Example:
    HOST = "tdt4225-00.idi.ntnu.no" // Your server IP address/domain name
    DATABASE = "testdb" // Database name, if you just want to connect to MySQL server, leave it empty
    USER = "testuser" // This is the user you created and added privileges for
    PASSWORD = "test123" // The password you set for said user
    """

    def __init__(self,
                 # HOST="tdt4225-13.idi.ntnu.no",
                 HOST="127.0.0.1",
                 PORT=3306,
                 DATABASE="exercise2",
                 # USER="gruppe13",
                 USER="root",
                 PASSWORD=os.getenv('PASSWORD')):
        # Connect to the database
        try:
            print("password: " + os.getenv('PASSWORD'))
            self.db_connection = mysql.connect(
                host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=PORT)
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        self.start_time = time.time()
        # Get the db cursor
        self.cursor = self.db_connection.cursor()

        print("Connected to:", self.db_connection.get_server_info())
        # get database information
        self.cursor.execute("select database();")
        database_name = self.cursor.fetchone()
        print("You are connected to the database:", database_name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # close the cursor
        self.cursor.close()
        # close the DB connection
        self.db_connection.close()
        print("\n-----------------------------------------------")
        print("Connection to %s is closed" %
              self.db_connection.get_server_info())

    def create_Table(self, query):
        self.cursor.execute(query)
        self.db_connection.commit()

    def drop_table(self, table):
        self.cursor.execute("DROP TABLE IF EXISTS %s" % table)
        self.db_connection.commit()

    def batch_insert_users(self, user_list):
        print("inserting users...")
        query = "INSERT INTO user (id, has_labels) VALUES (%s, %s)"
        self.cursor.executemany(query, user_list)
        self.db_connection.commit()
        print("finished insert!")

    def get_last_inserted_id(self):
        return self.cursor.lastrowid
    
    def total_distance(self):
        query = """SELECT lat, lon FROM trackpoint WHERE activity_id IN 
        (SELECT id FROM activity WHERE user_id = 084 AND transportation_mode = 'walk' 
        AND YEAR(start_date_time)='2008')"""
        self.cursor.execute(query)
        coordinates = self.cursor.fetchall()
        total_distance = 0
        for index in range(len(coordinates)):
            if index == len(coordinates)-1:
                break
            distance = haversine(coordinates[index], coordinates[index+1])
            total_distance += distance
        print(total_distance)    

    def insert_activity_with_id(self, activity):
        try:
            if(activity["transportation_mode"] != False):
                query = "INSERT INTO activity (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES ('%s', '%s', '%s', '%s', '%s')"
                self.cursor.execute(query % (activity["id"], 
                                            activity["user_id"], 
                                            activity["transportation_mode"], 
                                            activity["start_date_time"], 
                                            activity["end_date_time"]))
            else: 
                query = "INSERT INTO activity (id, user_id, start_date_time, end_date_time) VALUES ('%s', '%s', '%s', '%s')"
                self.cursor.execute(query % (activity["id"], 
                                            activity["user_id"], 
                                            activity["start_date_time"], 
                                            activity["end_date_time"]))
            self.db_connection.commit()
        except Exception as e:
            print(e)

    def insert_trackpoints_with_id(self, trackpoints):
        try:
            query = "INSERT INTO trackpoint (id, activity_id, lat, lon, altitude, date_days, date_time) VALUES {}".format(trackpoints)
            self.cursor.execute(query)
            self.db_connection.commit()
        except Exception as e:
            print(e)
    
    def update_activity_labels(self, labels):
        query = "UPDATE activity SET transportation_mode = null WHERE transportation_mode is not null"
        self.cursor.execute(query)
        self.db_connection.commit()

        for label in labels:
            query = "UPDATE activity SET transportation_mode = '%s' WHERE start_date_time = '%s' AND end_date_time = '%s' AND user_id = '%s'"
            self.cursor.execute(query % (label["transportation_mode"], label["start_time"], label["end_time"], label["user_id"]))
            self.db_connection.commit()