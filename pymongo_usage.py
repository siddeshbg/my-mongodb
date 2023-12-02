from pymongo import MongoClient
from dateutil import parser
from pandas import DataFrame
import urllib.parse
import os


def get_database(db_name):
    username = urllib.parse.quote_plus(os.environ['USER_NAME'])
    password = urllib.parse.quote_plus(os.environ['PASSWORD'])
    # provide monogodb atlas url
    CONNECTION_STRING = "mongodb+srv://%s:%s@cluster0.rosqiu8.mongodb.net/?retryWrites=true&w=majority" % (
    username, password)

    # create a connection using Mongoclient
    client = MongoClient(CONNECTION_STRING)

    # create the DB
    return client[db_name]


def create_collection(db_name, collection_name):
    return db_name[collection_name]

def insert_items(collection, items):

    collection.insert_many(items)

    # expiry_date = '2021-07-13T00:00:00.000Z'
    # expiry = parser.parse(expiry_date)
    # item_3 = {
    #     "item_name": "Bread",
    #     "quantity": 2,
    #     "ingredients": "all-purpose flour",
    #     "expiry_date": expiry
    # }
    #
    # collection_name.insert_one(item_3)


def query_db(collection_name):
    item_details = collection_name.find()
    item_details = collection_name.find({"category": "food"})
    # convert the dictionary objects to dataframe
    items_df = DataFrame(item_details)
    print(items_df)
    # for item in item_details:
    #     # print(item)
    #     print(item['item_name'], item['category'])
    # pass


def create_index(collection_name, field_name):
    # Create an index on the collection
    index = collection_name.create_index(field_name)


if __name__ == "__main__":
    # Get the database
    dbname = get_database(db_name='user_shopping_list')
    # collection_names = ['user_1_items', 'samskruthi_items']
    collection = create_collection(db_name=dbname, collection_name="user_1_items")

    item_1 = {
        "item_name": "Phone Holder",
        "max_discount": "10%",
        "batch_number": "RR450020FRG",
        "price": 500,
        "category": "Accessory"
    }

    item_2 = {
        "item_name": "Cake",
        "category": "food",
        "quantity": 1,
        "price": 1200,
        "item_description": "Unicorn theme chocolate flavor cake"
    }

    # insert_items(collection=collection, items=[item_1, item_2])

    create_index(collection_name=collection, field_name="category")

    query_db(collection_name=collection)

