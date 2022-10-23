from DbConnector import DbConnector
from Process_data import Process_data
import pprint as pp
from haversine import haversine
from datetime import datetime


COLLECTIONS = ["user", "activity", "trackpoint"]


def insert_data(db_connector):
    # Instantiating fresh collections
    for collection in COLLECTIONS:
        db_connector.remove_collection(collection)
        db_connector.create_collection(collection)
    db_connector.create_indexes()

    process_data = Process_data(db_connector)

    # Inserting data
    process_data.process()


def main():
    db_connector = DbConnector()

    # insert_data(db_connector)

    # Querying data
    # task2 = db_connector.find_average_activities_per_user()
    # print("Average activities per user: %s" % list(task2)[0]["avg"])

    # task3 = db_connector.find_top_20_users_with_most_activities()
    # print("User_id  | Activity count")
    # for user in list(task3):
    #     print("{:8} | {:8}".format(user["_id"], user["count"]))

    # task4 = db_connector.find_users_who_have_taken_taxi()
    # print("Users who have taken a taxi:")
    # for i in task4:
    #     print(i["_id"])

    # task5 = db_connector.find_all_types_of_transportation_modes()
    # print("Transportation mode | Count")
    # for i in task5:
    #     print("{:19} | {:8}".format(i["_id"], i["count"]))

    # task6a = db_connector.find_year_with_most_activities()
    # print("Year with most activities: %s" % list(task6a)[0]["_id"])

    # task6b = db_connector.find_year_with_most_recorded_hours()
    # print("Year with most recorded hours: %s" % list(task6b)[0]["_id"])

    def task7():
        activities = db_connector.find_all_activity_ids_for_user_112_in_2008()

        distance = 0
        old = (0, 0)
        for i in activities:
            trackpoints = db_connector.find_trackpoints_of_activity(i["_id"])
            for tp in trackpoints:
                if (old != (0, 0)):
                    new = (tp["location"]["coordinates"][1],
                           tp["location"]["coordinates"][0])
                    distance += haversine(old, new)
                    old = new
                else:
                    old = (tp["location"]["coordinates"][1],
                           tp["location"]["coordinates"][0])
        print("TOTAL DISTANCE: ", distance)
    #task7()

    # task8 = db_connector.find_top_20_users_who_have_gained_most_altitude_meters()
    # for i in task8:
    #     print(i)

    # TASK 8
    def task8():
        print("Task 8")

        user_ids = db_connector.find_all_user_ids()
        user_activity_ids = {}
        for user in user_ids:
            activities = db_connector.find_all_activity_ids_of_user(
                user["_id"])
            activity_ids = []
            for a in activities:
                activity_ids.append(a["_id"])
            user_activity_ids[user["_id"]] = activity_ids

        user_altitude = {}
        for user in user_activity_ids:
            user_altitude[user] = 0
            for activity in user_activity_ids[user]:
                activity_alt_gained = 0
                old_alt = -1000
                altitudes = db_connector.find_altitude_of_activity(activity)
                for alt in altitudes:
                    if alt["altitude"] == -777:
                        continue

                    new_alt = alt["altitude"]
                    if old_alt == -1000:
                        old_alt = new_alt
                    if new_alt > old_alt:
                        activity_alt_gained += new_alt - old_alt
                    old_alt = new_alt
                user_altitude[user] += activity_alt_gained*0.3048
                activity_alt_gained = 0
                
        sorted_user_altitudes = sorted(
            user_altitude.items(), key=lambda x: x[1], reverse=True)
        print("User_id  | Altitude gained")
        for key, altitude in sorted_user_altitudes:
            print("{:8} | {:8}".format(key, round(altitude)))
    #task8()

    # TASK 9
    def task9():
        user_ids = db_connector.find_all_user_ids()
        user_invalid_activities = {}
        for user in user_ids:
            print("Finding invalid actitities for user: " + user["_id"])
            activities = db_connector.find_all_activity_ids_of_user(
                user["_id"])
            user_invalid_activities[user["_id"]] = 0
            for a in activities:
                prev_tp = "first"
                for tp in db_connector.find_date_time_of_activity(a["_id"]):
                    if prev_tp == "first":
                        prev_tp = tp
                    else:
                        if check_if_invalid(prev_tp["date_time"], tp["date_time"]) == True:
                            user_invalid_activities[user["_id"]] += 1
                            break
                        prev_tp = tp
        sorted_user_invalid_activities = sorted(
            user_invalid_activities.items(), key=lambda x: x[1], reverse=True)
        print("User_id  | Invalid activities")
        for key, count in sorted_user_invalid_activities:
            print("{:8} | {:8}".format(key, count))

    # return the difference in seconds between two dates given with string format year-month-day hour:minute:second
    def check_if_invalid(date_time1, date_time2):
        date_time_obj1 = datetime.strptime(date_time1, '%Y-%m-%d %H:%M:%S')
        date_time_obj2 = datetime.strptime(date_time2, '%Y-%m-%d %H:%M:%S')
        diff = (date_time_obj2-date_time_obj1).total_seconds()
        #print("diff " + str(diff))
        if diff >= 300:
            return True
        else:
            return False
    #task9()

    # task10 = db_connector.find_users_with_activity_with_trackpoint_at_location()
    # for i in task10:
    #     print(i)

    task11 = db_connector.find_most_frequent_transportation_mode_for_each_user()
    for i in task11:
       print(i)


main()
