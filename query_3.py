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


def query_3(db):
    # get pairs film_id and category_id
    collection_film_category = db['film_category']
    film_category = []

    for film in collection_film_category.find({}):
        film_category.append({film.get('film_id'): film.get('category_id')})

    # get pairs inventory_id and film_id
    collection_inventory = db['inventory']
    inventory_dict = {}

    for inventory in collection_inventory.find({}):
        inventory_dict.update(
            {inventory.get('inventory_id'): inventory.get('film_id')})

    # get pairs category_id and catogory_name
    collection_category = db['category']
    category_dict = {}

    for film in collection_category.find({}):
        category_dict.update({film.get('category_id'): film.get('name')})

    # get films, its categories and number of times in rental
    report = []
    for film in film_category:
        film_id, category_id = film.popitem()
        category = category_dict.get(category_id)
        number_of_times = 0

        elements = inventory_dict.values()
        for element in elements:
            if film_id == element:
                number_of_times = number_of_times + 1

        report_item = {
            'film_id': film_id,
            'category': category,
            'number_of_times_in_rental': number_of_times
        }
        report.append(report_item)

    with open('query_3.csv', 'w') as csvfile:
        fieldnames = ['film_id', 'category', 'number_of_times_in_rental']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report)


start_time = time.time()
db = connect_mongodb()
query_3(db)
end_time = time.time()
print('Working time is', end_time-start_time, 'sec')
