
from DbConnector import DbConnector
from Process_data import Process_data


COLLECTIONS = ["user", "activity", "trackpoint"]


def main():
    db_connector = DbConnector()

    # Instantiating fresh collections
    for collection in COLLECTIONS:
        db_connector.remove_collection(collection)
        db_connector.create_collection(collection)
    db_connector.create_indexes()
    process_data = Process_data(db_connector)

    # Inserting data
    process_data.process()


main()
