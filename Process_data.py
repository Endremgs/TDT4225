import os
import pandas as pd
import numpy as np
BASE_PATH = "./dataset"


class Process_data:
    def __init__(self):
        self.users = {}
        self.users_with_labels = []
        self.activities = []
        self.trackpoints = []

    def read_users(self):
        path = BASE_PATH + "/Data"
        users = {}
        userIDs = []
        for file in os.listdir(path):
            userIDs.append(os.path.join(path, file)[-3:])
        # print(userIDs)
        for userID in userIDs:
            # print(userID)
            users[userID] = {}
            for root, dirs, files in os.walk(path+"/"+userID):
                if "/Trajectory" in root:
                    filesInfo = {
                        "path": root,
                        "files": files,
                        "has_label": self.user_has_label(userID)
                    }
                    users[userID].update(filesInfo)
        self.users = users
        return users

    def read_labeled_users(self):
        path = BASE_PATH + "/labeled_ids.txt"
        labeled_users = pd.read_csv(path, header=None).to_numpy()
        formatted_labeled_users = []
        for element in labeled_users:
            formatted_labeled_users.append('{:03}'.format(element[0]))
        self.users_with_labels = formatted_labeled_users
        return formatted_labeled_users

    # Todo - gjøre direkte i kode kanskje?
    def user_has_label(self, userID):
        return userID in self.users_with_labels

    # def read_activities(self):
    # TODO - kun labeled users har aktiviteter. Kan være hensiktsmessig å sjekke
    # Todo - has_label feltet for brukeren i starten av funksjonen
    def retrieve_activities_for_labeled_user(self, user):
        if user["has_label"]:
            activities = []
            with open(user["path"].replace("/Trajectory", "/labels.txt"), "r") as file:
                for line in file.readlines()[1:]:
                    # info = line.split()
                    activities.append(line.split())
            # print(activities)
            return activities
        return False

    def read_trajectory(self, userID, filename):
        trajectory_list = []
        path = BASE_PATH+"/Data/"+userID+"/Trajectory/"+filename
        trajectories = pd.read_csv(path, header=None, skiprows=6).to_numpy()
        if len(trajectories) < 2500:
            trajectory_list = [{"lat": trajectory[0],
                                "lon": trajectory[1],
                                "alt": trajectory[3],
                                "date": trajectory[5],
                                "time": trajectory[6]}
                            for trajectory in trajectories]
        #print(trajectory_list[0])
        return trajectory_list

    def find_matching_trajectory(self, label, user, userID):
        startTime = label[0] + " " + label[1]
        endTime = label[2] + " " + label[3]
        startTime = self.convert_timeformat(startTime)
        endTime = self.convert_timeformat(endTime)
        for fileName in user["files"]:
            matchingStartTime = False
            matchingEndTime = False
            #Check each trackpoint and check if they are a match
            for trackpoint in self.read_trajectory(userID, fileName):
                trackPointTime = trackpoint["date"] + " " + trackpoint["time"]
                if startTime == trackPointTime:
                    matchingStartTime = True
                if (endTime == trackPointTime) and startTime:
                    matchingEndTime = True

                if matchingStartTime and matchingEndTime:
                    print("Found match on activity '" + label[4] + "' With file '" + fileName + "'")
                    return fileName

    def convert_timeformat(self, date):
        return date.replace("/", "-")
        

def main():
    process_data = Process_data()
    process_data.read_labeled_users()
    process_data.read_users()
    print(process_data.users_with_labels)
    for userID in process_data.users_with_labels:
        print("Checking activities for user " + userID + "...")
        for item in process_data.retrieve_activities_for_labeled_user(process_data.users[userID]):
            process_data.find_matching_trajectory(item, process_data.users[userID], userID)
    print("FINISHED")
    #process_data.read_trajectory("175", "20071019052315.plt")


main()
