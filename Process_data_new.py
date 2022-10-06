import os
import pandas as pd
import numpy as np

BASE_PATH = "./dataset"

class Process_data_v2:
    def __init__(self, db_connector):
        # user:{id, has_labels}
        self.users = {}
        self.users_with_labels = []
        #activities: {user_id, transportation_mode, start_date_time, end_date_time, trackpoints:[{lat, lon, altitude, date_days, date_time}]}
        self.activities = []
        self.trackpoints = []
        self.activity_id_counter = 1
        self.trackpoint_id_counter = 1
        self.db_connector = db_connector
        
    # Read single user
    def read_user(self, user_id):
        path = BASE_PATH + "/Data"
        self.users[user_id] = {}
        for root, dirs, files in os.walk(path+"/"+user_id):
            root = root.replace("\\", "/") #For windows
            if "/Trajectory" in root:
                userInfo = {
                    "id": user_id,
                    "path": root,
                    "used_files": [],
                    "has_label": self.user_has_label(user_id),
                    "labels" : {}
                }
                self.users[user_id].update(userInfo)

    def read_user_activities(self, user_id):
        user = self.users[user_id]
        for filename in os.listdir(user["path"]):
            if filename.endswith(".plt"):
                path = user["path"] + "/" + filename
                activity_trackpoints = np.genfromtxt(path, skip_header=6, skip_footer=1, delimiter=',',dtype=str, usecols=(0,1,3,5,6))
                if(len(activity_trackpoints) >= 2500):
                    continue
                res = self.match_label(activity_trackpoints, user_id)
                activity = {"id": int(self.activity_id_counter),
                            "user_id": str(user_id), 
                            "transportation_mode": res,
                            "start_date_time": activity_trackpoints[0][3] + " " + activity_trackpoints[0][4],
                            "end_date_time": activity_trackpoints[-1][3] + " " + activity_trackpoints[-1][4]}
                self.activities.append(activity)
                self.db_connector.insert_activity_with_id_v2(activity)

                trackpoints = []
                for trackpoint in activity_trackpoints:
                    trackpoint_date_days = float(trackpoint[3][-2])
                    trackpoint_date_time = trackpoint[3] + \
                            " " + trackpoint[4]
                    new_item = (int(self.trackpoint_id_counter),
                                int(self.activity_id_counter),
                                float(trackpoint[0]),
                                float(trackpoint[1]), 
                                float(trackpoint[2]), 
                                trackpoint_date_days, 
                                trackpoint_date_time)
                    self.trackpoint_id_counter += 1
                    trackpoints.append(new_item)
                values = ', '.join(map(str, trackpoints))
                self.db_connector.insert_trackpoints_with_id(values)
                self.activity_id_counter += 1


    # Find all users with labels
    def find_users_with_labels(self):
        path = BASE_PATH + "/labeled_ids.txt"
        labeled_users = pd.read_csv(path, header=None).to_numpy()
        formatted = []
        for element in labeled_users:
            formatted.append('{:03}'.format(element[0]))
        self.users_with_labels = formatted
    
    # Take in labels and activity, check if it matches
    def match_label(self, activity, user_id):
        user = self.users[user_id]
        if(user["has_label"]):
            first_trackpoint = activity[0]
            last_trackpoint = activity[-1]
            starttime = first_trackpoint[3] + \
                        " " + first_trackpoint[4]
            endtime = last_trackpoint[3] + \
                        " " + last_trackpoint[4]
            labels = user["labels"]
            if starttime in self.users[user_id]["labels"]:
                if endtime == labels[starttime]["end_time"]:
                    print("Match " + labels[starttime]["type"])
                    return labels[starttime]["type"]
        return False

    # Read labels, return labels as a dictionary
    def read_labels(self, user_id):
        user = self.users[user_id]
        label_dict = {}
        if user["has_label"]:
            with open(user["path"].replace("/Trajectory", "/labels.txt"), "r") as file:
                labels = file.readlines()[1:]
                for line in labels:
                    label = line.split()
                    start = self.convert_timeformat(label[0] + " " + label[1])
                    label_dict[start] = {}
                    end = self.convert_timeformat(label[2] + " " + label[3])
                    input = {"end_time": end, "type": label[4]}
                    label_dict[start] = input
            self.users[user_id]["labels"] = label_dict

                   
                
    def convert_timeformat(self, date):
        return date.replace("/", "-")

    def user_has_label(self, userID):
        if len(self.users_with_labels) == 0:
            self.read_labeled_users()
        return userID in self.users_with_labels

    def insert_activities(self, activity):
        for activity in self.activities:
            self.db_connector.insert_activity(activity)

    # Read all users and save data
    def read_all_users(self):
        self.find_users_with_labels()
        path = BASE_PATH + "/Data"
        self.users = {}
        userIDs = []
        for file in os.listdir(path):
            userIDs.append(os.path.join(path, file)[-3:])
        for userID in userIDs:
            #print(userID)
            self.read_user(userID)

    # Main function
    def process(self):
        self.read_all_users()
        #Reformat users
        user_list = []
        for user in self.users.values():
            if (user.get("id")):
                user_list.append((user["id"], int(user["has_label"])))
        self.db_connector.batch_insert_users(user_list)

        for user in self.users.values():
            print("reading user: "+ user["id"])
            if(user["has_label"]):
                self.read_labels(user["id"])
            self.read_user_activities(user["id"])

        #self.db_connector.batch_insert_activities_with_id(self.activities)
        #self.db_connector.batch_insert_trackpoints_with_id(self.trackpoints)
        

            
#def main():
    #process_data = Process_data_v2()
    #process_data.process()
    #print("Activities: " + str(len(process_data.activities)))
    #print("Trackpoints: " + str(len(process_data.trackpoints)))
    #activity = np.genfromtxt("./dataset/Data/128/Trajectory/20070414005628.plt", skip_header=6, delimiter=',',dtype=str, usecols=(0,1,3,5,6))
    #print(activity)
#main()