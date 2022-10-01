import mysql.connector as mysql
# from decouple import config
import os
from dotenv import load_dotenv

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
                 HOST="tdt4225-13.idi.ntnu.no",
                 DATABASE="exercise2",
                 USER="gruppe13",
                 PASSWORD=os.getenv('PASSWORD')):
        # Connect to the database
        try:
            print("password: " + os.getenv('PASSWORD'))
            self.db_connection = mysql.connect(
                host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=3306)
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

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

    def insert_user(self, user):
        # print(user)
        query = "INSERT INTO user (id, has_labels) VALUES ('%s', '%s')"
        self.cursor.execute(query % (user["id"], int(user["has_label"])))
        self.db_connection.commit()
        print("inserted user with ID: " + str(user["id"]))

    def insert_activity(self, activity):
        if ('transporation_mode' in activity.keys()):
            query = "INSERT INTO activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES ('%s', '%s', '%s', '%s')"
            self.cursor.execute(query % (
                activity["user_id"], activity["transportation_mode"], activity["start_date_time"], activity["end_date_time"]))
            self.db_connection.commit()
        else:
            query = "INSERT INTO activity (user_id, start_date_time, end_date_time) VALUES ('%s', '%s', '%s')"
            self.cursor.execute(query % (
                activity["user_id"], activity["start_date_time"], activity["end_date_time"]))
            self.db_connection.commit()
        print("inserted activity:")
        print(activity)

    def insert_trackpoint(self, trackpoint):
        print(trackpoint)
        query = "INSERT INTO trackpoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES ('%s', '%s', '%s', '%s', '%s', '%s')"
        trackpoint_date_days = float(trackpoint["date"][-2])
        trackpoint_date_time = trackpoint["date"] + " " + trackpoint["time"]
        self.cursor.execute(query % (trackpoint["activity_id"], float(trackpoint["lat"]),
                            float(trackpoint["lon"]), int(trackpoint["alt"]), trackpoint_date_days, trackpoint_date_time))
        self.db_connection.commit()

    def batch_insert_users(self, user_list):
        print("inserting users...")
        query = "INSERT INTO user (id, has_labels) VALUES (%s, %s)"
        self.cursor.executemany(query, user_list)
        self.db_connection.commit()
        print("finished")

    def batch_insert_trackpoints(self, trackpoint_list):
        print("inserting trackpoint_list...")
        query = "INSERT INTO trackpoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
        self.cursor.executemany(query, user_list)
        self.db_connection.commit()
        print("finished")

    def get_last_inserted_id(self):
        return self.cursor.lastrowid
