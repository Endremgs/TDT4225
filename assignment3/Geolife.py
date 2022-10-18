from DbConnector import DbConnector
from Process_data import Process_data
import pprint as pp


COLLECTIONS = ["user", "activity", "trackpoint"]


def main():
    db_connector = DbConnector()

    # Instantiating fresh collections
    # for collection in COLLECTIONS:
    #     db_connector.remove_collection(collection)
    #     db_connector.create_collection(collection)
    # db_connector.create_indexes()
    # process_data = Process_data(db_connector)

    # Inserting data
    # process_data.process()

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

    # task7 = db_connector.find_total_distance_walked_for_user_112_in_2008()
    # for i in task7:
    #     print(i)

    # task8 = db_connector.find_top_20_users_who_have_gained_most_altitude_meters()
    # for i in task8:
    #     print(i)

    # task9 = db_connector.find_all_users_who_have_invalid_activities()
    # for i in task9:
    #     print(i)

    # task10 = db_connector.find_users_who_have_been_to_location()
    # for i in task10:
    #     print(i)

    task11 = db_connector.find_all_users_who_have_registered_transportation_mode()
    for i in task11:
        print(i)


main()
