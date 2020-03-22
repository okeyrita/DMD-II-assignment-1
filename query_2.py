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


def query_2(db):
    collection_film_actor = db['film_actor']
    actors = {}
    actor_ids = []
    number_of_actors = 0
    # get pairs actor_id : list_of films wirh this actor
    for film_actor in collection_film_actor.find({}):
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

    # get table of number of movies for actors which co-starred
    relations = []
    for actor_id in list(dict.fromkeys(actor_ids)):
        relation = {'actor_id': actor_id}
        for actor_id_another in actor_ids:
            if actor_id == actor_id_another:
                num_of_movies = '-'
            else:
                film_id_list = actors.get(actor_id)
                film_id_another_list = actors.get(actor_id_another)

                num_of_movies = 0
                for film in film_id_list:
                    if film in film_id_another_list:
                        num_of_movies = num_of_movies + 1

            relation.update({actor_id_another: num_of_movies})
        relations.append(relation)

    with open('query_2.csv', 'w') as csvfile:
        fieldnames = list(relations[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(relations)


start_time = time.time()
db = connect_mongodb()
query_2(db)
end_time = time.time()
print('Working time is', end_time-start_time, 'sec')
