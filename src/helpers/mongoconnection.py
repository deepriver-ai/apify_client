import os

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_CREDENTIALS = {
    "user": os.getenv("MONGO_USER"),
    "pass": os.getenv("MONGO_PASSWORD"),
    "host": os.getenv("MONGO_HOST"),
    "port": os.getenv("MONGO_PORT"),
    "authdb": os.getenv("MONGO_AUTHDB"),
}


def get_mongo_connection(credentials):
    connection_string = "mongodb://{user}:{pass}@{host}:{port}/{authdb}".format(**credentials)
    client = MongoClient(connection_string, connect=False)
    return client


mongoconn = get_mongo_connection(MONGO_CREDENTIALS)
