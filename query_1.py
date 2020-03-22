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


# query 1
def query_1(db):
    collection_customer = db['customer']
    collection_rental = db['rental']
    collection_inventory = db['inventory']
    collection_film_category = db['film_category']

    inventory = collection_inventory.find({})
    # find last year of rents
    last_year = 0
    for element in inventory:
        last_update = int(
            re.search(r'\d\d\d\d', element.get('last_update')).group(0))
        if last_year < last_update:
            last_year = last_update

    # get pairs film_id : category_id
    film_category = {}
    for film in collection_film_category.find({}):
        film_category.update({film.get('film_id'): film.get('category_id')})

    # get pairs inventory_id : film_id
    inventory_dict = {}
    for inventory in collection_inventory.find({}):
        inventory_dict.update(
            {inventory.get('inventory_id'): inventory.get('film_id')})

    # get pairs customer_id : inventory_id
    rentals = []
    for rental in collection_rental.find({}):
        rentals.append({rental.get('customer_id'): rental.get('inventory_id')})

    # get list of customer_id
    customers_list = []
    for customer in collection_customer.find({}):
        customers_list.append(customer.get('customer_id'))

    
    customers = []
    for customer in customers_list:
        categories = set()
        for rental in rentals:
            cust_id, invent_id = rental.copy().popitem()

            if customer == cust_id:
                # get inventory_id -> film_id -> category_id 
                categories.add(film_category.get(
                    inventory_dict.get(invent_id)))

        if len(categories) >= 2:
            customers.append(customer)

    print(customers)


start_time = time.time()
db = connect_mongodb()
query_1(db)
end_time = time.time()
print('Working time is', end_time-start_time, 'sec')
