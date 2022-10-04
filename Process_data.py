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

    # Read all users (labeled and unlabeled) and stores in self.users
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

    # Helper function to check if user with given userID has labels.
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
            filenames = os.listdir(user["path"])
            for filename in filenames:

                trackpoint_list = self.read_trajectory(user["path"], filename)
                # Skipping activities with less than 2500 trackpoints
                if (len(trackpoint_list) == 0):
                    continue

                # Create activity without transportation_mode
                activity = {
                    "user_id": user["id"],
                    "start_date_time": trackpoint_list[0][3] + " " + trackpoint_list[0][4],
                    "end_date_time": trackpoint_list[-1][3] + " " + trackpoint_list[-1][4]}

                # Insert activity & retrieve id
                self.db_connector.insert_activity(activity)
                activity_id = self.db_connector.get_last_inserted_id()

                # Insert each trackpoint and connect to activity
                for index, trackpoint in enumerate(trackpoint_list):
                    trackpoint_date_days = float(trackpoint[3][-2])
                    trackpoint_date_time = trackpoint[3] + \
                        " " + trackpoint[4]
                    trackpoint_list[index] = (int(activity_id), float(trackpoint[0]), float(
                        trackpoint[1]), int(trackpoint[2]), trackpoint_date_days, trackpoint_date_time)
                # Batch insert trackpoints
                print("batch inserting for activity " + str(activity_id))
                self.db_connector.batch_insert_trackpoints(trackpoint_list)
        else:
            raise Exception("User possibly has labels")

    # Add all activities for a user which has labels
    def retrieve_activities_for_user_with_labels(self, user):
        if user["has_label"]:
            with open(user["path"].replace("/Trajectory", "/labels.txt"), "r") as file:
                labels = file.readlines()[1:]
                for line in labels:
                    label = line.split()
                    print(
                        "checking for matching labels and files for user: " + user["id"])
                    print("Labels to check: " + str(len(labels)))
                    self.find_matching_trajectory(label, user)

            # Create activitites for remainding plt files that were not matched to label
            for filename in user["files"]:
                trackpoint_list = self.read_trajectory(user["path"], filename)
                # Skipping activities with less than 2500 trackpoints
                if (len(trackpoint_list) == 0):
                    continue
                activity = {
                    "user_id": user["id"],
                    "start_date_time": trackpoint_list[0][3] + " " + trackpoint_list[0][4],
                    "end_date_time": trackpoint_list[-1][3] + " " + trackpoint_list[-1][4]}

                # Insert activity & retrieve id
                self.db_connector.insert_activity(activity)
                activity_id = self.db_connector.get_last_inserted_id()

                # Insert each trackpoint and connect to activity
                for index, trackpoint in enumerate(trackpoint_list):
                    trackpoint_date_days = float(trackpoint[3][-2])
                    trackpoint_date_time = trackpoint[3] + \
                        " " + trackpoint[4]
                    trackpoint_list[index] = (int(activity_id), float(trackpoint[0]), float(
                        trackpoint[1]), int(trackpoint[2]), trackpoint_date_days, trackpoint_date_time)

                # Batch insert trackpoints
                print("batch inserting for activity " + str(activity_id))
                self.db_connector.batch_insert_trackpoints(trackpoint_list)

        else:
            raise Exception("User does not have label")

    def read_trajectory(self, user_path, filename):
        trackpoint_list = []
        path = user_path+"/"+filename
        trajectories = pd.read_csv(path, header=None, skiprows=6).to_numpy()
        if len(trajectories) <= 2500:
            trackpoint_list = [(trajectory[0],
                               trajectory[1],
                               trajectory[3],
                               trajectory[5],
                               trajectory[6])
                               for trajectory in trajectories]
        return trackpoint_list

    # Takes in a label for a user
    # Then return the activity(plt file) with a matching start and end time
    def find_matching_trajectory(self, label, user):
        label_start_time = self.convert_timeformat(label[0] + " " + label[1])
        label_end_time = self.convert_timeformat(label[2] + " " + label[3])

        print("files to check: " + str(len(user["files"])))
        for index, fileName in enumerate(user["files"]):
            matching_start_time = False
            matching_end_time = False

            trackpoint_list = self.read_trajectory(user["path"], fileName)

            # Skipping activities with less than 2500 trackpoints
            if (len(trackpoint_list) > 0):

                # Check if start time is a match, skipping otherwise
                first_trackpoint = trackpoint_list[0]
                first_track_point_time = first_trackpoint[3] + \
                    " " + first_trackpoint[4]
                if first_track_point_time == label_start_time:
                    matching_start_time = True
                else:
                    continue

                # Check if end time is a match, skipping otherwise
                last_trackpoint = trackpoint_list[-1]
                last_track_point_time = last_trackpoint[3] + \
                    " " + last_trackpoint[4]
                if last_track_point_time == label_end_time:
                    matching_end_time = True
                else:
                    continue

                if matching_start_time and matching_end_time:
                    print("found match for user: " + str(user["id"]))
                    # Insert activity
                    activity = {
                        "user_id": user["id"],
                        "transportation_mode": label[4],
                        "start_date_time": label_start_time,
                        "end_date_time": label_end_time}
                    self.db_connector.insert_activity(activity)
                    activity_id = self.db_connector.get_last_inserted_id()

                    # Insert each trackpoint and connect to activity
                    for idx, trackpoint in enumerate(trackpoint_list):
                        trackpoint_date_days = float(trackpoint[3][-2])
                        trackpoint_date_time = trackpoint[3] + \
                            " " + trackpoint[4]
                        trackpoint_list[idx] = (int(activity_id), float(trackpoint[0]), float(
                            trackpoint[1]), int(trackpoint[2]), trackpoint_date_days, trackpoint_date_time)
                    # Batch insert trackpoints
                    print("batch inserting for activity " + str(activity_id))
                    self.db_connector.batch_insert_trackpoints(trackpoint_list)

                    # Remove the file from the list of files to check
                    user["files"].pop(index)

    # Helper function to fix time format
    def convert_timeformat(self, date):
        return date.replace("/", "-")

    # Main function for initializing the processing and insertion of data
    def process(self):
        self.read_users()
        user_list = []

        for user in self.users.values():
            if (user.get("id")):
                user_list.append((user["id"], int(user["has_label"])))
        self.db_connector.batch_insert_users(user_list)

        for user in self.users.values():
            if (user.get("has_label")):
                if user["has_label"]:
                    self.retrieve_activities_for_user_with_labels(user)
                else:
                    self.retrieve_activities_for_unlabeled_user(user)
