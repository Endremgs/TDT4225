import time
import os
import pandas as pd
import numpy as np


BASE_PATH = "./dataset"


class Process_data:
    def __init__(self, db_connector):
        self.users = {}
        self.users_with_labels = []
        self.activities = []
        self.trackpoints = []
        self.db_connector = db_connector

    # Read all users (labeled and unlabeled) and save to self.users
    def read_users(self):
        self.read_labeled_users()
        path = BASE_PATH + "/Data"
        users = {}
        userIDs = []
        for file in os.listdir(path):
            userIDs.append(os.path.join(path, file)[-3:])
        for userID in userIDs:
            users[userID] = {}
            for root, dirs, files in os.walk(path+"/"+userID):
                if "/Trajectory" in root:
                    userInfo = {
                        "id": userID,
                        "path": root,
                        "files": files,
                        "has_label": self.user_has_label(userID)
                    }
                    users[userID].update(userInfo)
                    # Query for inserting user into database
                    # self.db_connector.insert_user(userInfo)
        self.users = users
        return users

    # Read all labeled userIDs and save to self.users_with_labels
    def read_labeled_users(self):
        path = BASE_PATH + "/labeled_ids.txt"
        labeled_users = pd.read_csv(path, header=None).to_numpy()
        formatted_labeled_users = []
        for element in labeled_users:
            formatted_labeled_users.append('{:03}'.format(element[0]))
        self.users_with_labels = formatted_labeled_users
        return formatted_labeled_users

    # Check if user with userID has labels.
    def user_has_label(self, userID):
        if len(self.users_with_labels) == 0:
            self.read_labeled_users()
        return userID in self.users_with_labels

    # Adds all activities of unlabeled users to self.activities
    # For each plt file in user trajectory:
    #   Create empty activity
    #   Connect all trackpoints within the plt file to the activity
    def retrieve_activities_for_unlabeled_user(self, user):
        if not user["has_label"]:
            #activities = []
            filenames = os.listdir(user["path"])
            for filename in filenames:
                # Insert activity without transportation_mode
                trackpoint_list = self.read_trajectory(user["path"], filename)
                activity = {
                    "user_id": user["id"],
                    # "transportation_mode": "",
                    "start_date_time": trackpoint_list[0]["date"] + " " + trackpoint_list[0]["time"],
                    "end_date_time": trackpoint_list[-1]["date"] + " " + trackpoint_list[-1]["time"]}
                # activities.append(activity)

                # Insert activity
                self.db_connector.insert_activity(activity)
                activity_id = self.db_connector.get_last_inserted_id()

                # Insert each trackpoint and connect to activity
                for trackpoint in trackpoint_list:
                    trackpoint["activity_id"] = activity_id
                    self.db_connector.insert_trackpoint(trackpoint)

            # return activities
        else:
            raise Exception("User possibly has labels")

    # Finds all activities that are labeled for a given user
        #

    def retrieve_activities_with_labels(self, user):
        if user["has_label"]:
            #activities = []
            with open(user["path"].replace("/Trajectory", "/labels.txt"), "r") as file:
                for line in file.readlines()[1:]:
                    label = line.split()
                    # print(user)
                    self.find_matching_trajectory(label, user)
                    # activities.append(activity)

            # Create activitites for remainding plt files that were not matched to label
            for filename in user["files"]:
                trackpoint_list = self.read_trajectory(user["path"], filename)
                activity = {
                    "user_id": user["id"],
                    "start_date_time": trackpoint_list[0]["date"] + " " + trackpoint_list[0]["time"],
                    "end_date_time": trackpoint_list[-1]["date"] + " " + trackpoint_list[-1]["time"]}
                self.db_connector.insert_activity(activity)
            # return activities
        else:
            raise Exception("User does not have label")

    def read_trajectory(self, user_path, filename):
        trackpoint_list = []
        path = user_path+"/"+filename
        trajectories = pd.read_csv(path, header=None, skiprows=6).to_numpy()
        if len(trajectories) < 2500:
            trackpoint_list = [{"lat": trajectory[0],
                                "lon": trajectory[1],
                                "alt": trajectory[3],
                                "date": trajectory[5],
                                "time": trajectory[6]}
                               for trajectory in trajectories]
        return trackpoint_list

    # Take in a label
    # Then return the activity(plt file) with a matching start and end time
    def find_matching_trajectory(self, label, user):
        label_start_time = self.convert_timeformat(label[0] + " " + label[1])
        label_end_time = self.convert_timeformat(label[2] + " " + label[3])

        # print(user)
        for index, fileName in enumerate(user["files"]):
            matching_start_time = False
            matching_end_time = False
            # Check each trackpoint and check if they are a match
            trackpoint_list = []

            for trackpoint in self.read_trajectory(user["path"], fileName):

                track_point_time = trackpoint["date"] + \
                    " " + trackpoint["time"]

                # Case to skip matching for optimization
                if not matching_start_time and time.strptime(track_point_time, "%Y-%m-%d %H:%M:%S") > time.strptime(label_start_time, "%Y-%m-%d %H:%M:%S"):
                    break
                else:
                    if matching_start_time:
                        # Appending all trackpoints after start time
                        trackpoint_list.append(trackpoint)

                    if label_start_time == track_point_time:
                        matching_start_time = True
                        # Appending first trackpoint
                        trackpoint_list.append(trackpoint)
                    # Else to avoid extra checks
                    else:
                        # Todo - could these two if statements be combined as they are both checking the same thing?
                        if (label_end_time == track_point_time) and matching_start_time:
                            matching_end_time = True

                        if matching_start_time and matching_end_time:
                            print("Found match on activity '" +
                                  label[4] + "' With file '" + fileName + "'")
                            user["files"].pop(index)
                            # Insert activity with transportation_mode, retrieving ID of activity

                            self.db_connector.insert_activity({
                                "user_id": user["id"],
                                "transportation_mode": label[4],
                                "start_date_time": trackpoint_list[0]["date"] + " " + trackpoint_list[0]["time"],
                                "end_date_time": trackpoint_list[-1]["date"] + " " + trackpoint_list[-1]["time"]})

                            activity_id = self.db_connector.get_last_inserted_id()
                            # for each trackpoint in trackpoint_list: insert trackpoint and connect to activity
                            for trackpoint in trackpoint_list:
                                trackpoint["activity_id"] = activity_id
                                self.db_connector.insert_trackpoint(trackpoint)
                            # print(trackpoint_list)
                            break
                            # return fileName

    def convert_timeformat(self, date):
        return date.replace("/", "-")

    # Iterate through all labels of the user
    # Then connect each unmatched activity with a empty activity
    def process_labeled_users(self):
        for user_id in self.users_with_labels:
            print("Checking activities for user " + user_id + "...")
            for item in self.retrieve_activity_labels(self.users[user_id]):
                print("bob")

    def process(self):
        self.read_users()
        user_list = []

        for user in self.users.values():
            print(user)
            if (user.get("id")):
                user_list.append((user["id"], int(user["has_label"])))
        self.db_connector.batch_insert_users(user_list)

        for user in self.users.values():
            print(user)
            if user["has_label"]:
                self.retrieve_activities_with_labels(user)
            else:
                self.retrieve_activities_for_unlabeled_user(user)

# def main():
#     process_data = Process_data()
#     process_data.read_labeled_users()
#     process_data.read_users()
#     # print(process_data.users_with_labels)
#     # process_data.retrieve_activities_for_unlabeled_user(
#     #   process_data.users["001"])
#     # length = 0
#     # for userID in process_data.read_labeled_users():
#     #     # print(process_data.users[userID])

#     #     activities = process_data.retrieve_activity_labels(
#     #         process_data.users[userID])
#     #     length += len(activities)

#     #     # print(activities)
#     # # print(length)

#     # file_count = 0
#     # for userID in process_data.read_labeled_users():
#     #     file_count += len(os.listdir('dataset/Data/'+userID+'/Trajectory'))

#     # print(file_count)

#     for userID in process_data.users_with_labels:
#         print("Checking activities for user " + userID + "...")
#         for item in process_data.retrieve_activities_with_labels(process_data.users[userID]):
#             process_data.find_matching_trajectory(
#                 item, process_data.users[userID])
#     print("FINISHED")
#     process_data.read_trajectory("175", "20071019052315.plt")


# main()
