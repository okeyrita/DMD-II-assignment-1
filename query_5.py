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

def query_5(db):

    # get actors degree using BFS
    def find_degrees(id, length):
        actors_degrees = []

        for film in actors.get(id):
            for actor in film_actors.get(film):
                if actor not in report:
                    actors_degrees.append(actor)
                    report.update({actor: length})

        for actor in actors_degrees:
            find_degrees(actor, length+1)

    # get actor_id from terminal line
    certain_actor_id = int(input('Enter the ID of certain actor:'))

    collection_fim_actor = db['film_actor']
    actors = {}
    actor_ids = []
    number_of_actors = 0
    # get pairs actor_id : list_of films wirh this actor
    for film_actor in collection_fim_actor.find({}):
        number_of_actors = number_of_actors + 1
        actor_id = film_actor.get('actor_id')
        actor_ids.append(actor_id)
        film_id = film_actor.get('film_id')

        element = actors.get(actor_id, None)
        if element == None:
            list_of_film_id = []
        else:
            list_of_film_id = element

        list_of_film_id.append(film_id)
        actors.update({actor_id: list_of_film_id})



    # get get pairs film_id : ist of actor_id from this film
    film_actors = {}
    for film_actor in collection_fim_actor.find({}):
        number_of_actors = number_of_actors + 1
        actor_id = film_actor.get('actor_id')
        actor_ids.append(actor_id)
        film_id = film_actor.get('film_id')

        element = film_actors.get(film_id, None)
        if element == None:
            list_of_film_id = []
        else:
            list_of_film_id = element

        list_of_film_id.append(actor_id)
        film_actors.update({film_id: list_of_film_id})


    report = {}
    report.update({certain_actor_id: 0})

    # get dictionary with degrees
    find_degrees(certain_actor_id, 1)

    with open('query_5.csv', 'w') as csvfile:
        fieldnames = ['actor_id', 'degrees']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for actor_id in list(actors.keys()):
            writer.writerow(
                {'actor_id': actor_id, 'degrees': report.get('actor_id')})

start_time = time.time()
db = connect_mongodb()
query_5(db)
end_time = time.time()
print('Working time is', end_time-start_time, 'sec')
