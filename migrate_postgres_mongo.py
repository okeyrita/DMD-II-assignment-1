import pymongo
import psycopg2
import json
import datetime
import decimal
import re
import csv
import time


# names of tables in postgres
list_of_tables_names = [
    'actor', 'address', 'category', 'city',
    'country', 'customer', 'film', 'film_actor',
    'film_category', 'inventory', 'language', 'payment',
    'rental', 'staff', 'store']


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

# connect to postgresql
def connect_postgresql():
    connection = psycopg2.connect(dbname='dvdrental', user='postgres',
                                  port="5432", password='luck', host='localhost')
    cursor = connection.cursor()
    return cursor

# migrate data from postgresql to mongodb
def migrate_db(db, cursor):
    for table_name in list_of_tables_names:

        mongo_collection = db[table_name]

        get_column_names_query = "Select * FROM " + table_name + " LIMIT 0"
        cursor.execute(get_column_names_query)
        column_names = [desc[0] for desc in cursor.description]

        get_content_of_table_query = "select * from " + table_name
        cursor.execute(get_content_of_table_query)

        records = cursor.fetchall()
        # take a row from table
        for i, row in enumerate(records):

            json_object = {}
            for a, column in zip(row, column_names):
                if type(a) is datetime.date:
                    a = datetime.date.strftime(a, "%d.%m.%Y")
                elif type(a) is datetime.datetime:
                    a = datetime.datetime.strftime(a, "%d.%m.%Y %H:%M:%S:%f")
                elif type(a) is decimal.Decimal:
                    a = float(a)
                elif type(a) is memoryview:
                    a = bytes(a)
                json_object.update({column: a})

            mongo_collection.insert(json_object)


start_time = time.time()
db = connect_mongodb()
cursor = connect_postgresql()
migrate_db(db, cursor)
end_time = time.time()
print('Working time is', end_time-start_time, 'sec')
