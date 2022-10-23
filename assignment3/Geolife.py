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
    # for i in task2:
    #     print(i)

    # task3 = db_connector.find_top_20_users_with_most_activities()
    # for i in task3:
    #     print(i)

    # task4 = db_connector.find_users_who_have_taken_taxi()
    # for i in task4:
    #     print(i)

    # task5 = db_connector.find_all_types_of_transportation_modes()
    # for i in task5:
    #     print(i)

    # task6a = db_connector.find_year_with_most_activities()
    # for i in task6a:
    #     print(i)

    # task6b = db_connector.find_year_with_most_recorded_hours()
    # for i in task6b:
    #     print(i)

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
    task7()

    # task8 = db_connector.find_top_20_users_who_have_gained_most_altitude_meters()
    # for i in task8:
    #     print(i)

    # TASK 8
    # Find top 20 users with the most altitude gained
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
            #print(user, user_altitude[user])

        sorted_user_altitudes = sorted(
            user_altitude.items(), key=lambda x: x[1], reverse=True)
        for i in range(20):
            print(sorted_user_altitudes[i])
    # task8()

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
    # task9()

    #task10 = db_connector.find_users_with_activity_with_trackpoint_at_location()

    #task11 = db_connector.find_all_users_who_have_registered_transportation_mode()
    # for i in task11:
    #    print(i)


main()
