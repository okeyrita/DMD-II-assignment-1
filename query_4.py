import pymongo
import psycopg2
import json
import datetime
import decimal
import re
import csv
import time


# connect to mongodb
def connect_mongodb():
    mongodb_settings = {
        'uri': 'mongodb://localhost:27017/',
        'db': 'dvdrental',
    }
    mongo_uri = mongodb_settings.get('uri', ''),
    mongo_db = mongodb_settings.get('db', ''),
    mongo_usr = mongodb_settings.get('usr', ''),
    mongo_pwd = mongodb_settings.get('pwd', ''),

    client = pymongo.MongoClient(mongo_uri)
    db = client['dvdrental']

    return db


def query_4(db):
    collection_customer = db['customer']
    collection_rental = db['rental']
    collection_inventory = db['inventory']

    # get pairs customer_id : store_id
    customers = {}
    for customer in collection_customer.find({}):
        customers.update({customer.get('customer_id'): customer.get('store_id')})

    # get pairs customer_id : inventory_id
    rentals = []
    for rental in collection_rental.find({}):
        rentals.append({rental.get('customer_id'): rental.get('inventory_id')})

    # get pairs inventory_id : film_id
    inventory = {}
    for invent in collection_inventory.find({}):
        inventory.update({invent.get('inventory_id'): invent.get('film_id')})

    # get pairs customer_id : list_of_films which current customer see
    dict_for_films = {}
    for customer_id in list(customers.keys()):
        list_of_films = []
        for rental in rentals.copy():
            invetory_id = rental.get(customer_id, None)
            if invetory_id != None:
                if customer_id == customer:
                    list_of_films.append(inventory.get(invetory_id))
        dict_for_films.update({customer_id: list_of_films})


    # get report with customer_id , their films , and list of recommended films
    report = []
    for customer_id in list(customers.keys()):
        recommends = []
        for another_customer_id in list(customers.keys()):
            if customer_id != another_customer_id:
                current_list = dict_for_films.get(customer_id)
                other_list = dict_for_films.get(another_customer_id)
                num_of_duplicates = 0
                for element in current_list:
                    if element in other_list:
                        num_of_duplicates = num_of_duplicates + 1

                # recommend films of another customer if 33% of these customers interected
                if num_of_duplicates >= len(current_list)/3:
                    for element in other_list:
                        if element not in current_list:
                            recommends.append(element)

        recommends = list(dict.fromkeys(recommends))
        report_item = {
            'customer_id': customer_id,
            'list_of_rented_movies': list_of_films,
            'recommends': recommends}

        report.append(report_item)

    with open('query_4.csv', 'w') as csvfile:
        fieldnames = ['customer_id', 'list_of_rented_movies', 'recommends']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report)


start_time = time.time()
db = connect_mongodb()
query_4(db)
end_time = time.time()
print('Working time is', end_time-start_time, 'sec')
