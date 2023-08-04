import streamlit as st
from streamlit.connections import ExperimentalBaseConnection
from streamlit.runtime.caching import cache_data

from pymongo import MongoClient
from pymongo.collection import Collection

class MongoDBConnection(ExperimentalBaseConnection[MongoClient]):

    def __init__(self, db_name, collection_name, **kwargs):
        super().__init__(**kwargs)
        self.db_name = db_name
        self.collection_name = collection_name

    def _connect(self, MONGODB_USERNAME, MONGODB_PASSWORD, CLUSTER_ADDRESS, **kwargs) -> MongoClient:
        CONNECTION_STRING = f"mongodb+srv://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@{CLUSTER_ADDRESS}/?retryWrites=true&w=majority"
        return MongoClient(CONNECTION_STRING, connect=False)

    def cursor(self) -> Collection:
        db = self._instance[self.db_name]
        return db[self.collection_name]

    def query(self, query, ttl = 3600) -> dict:
        collection = self.cursor()
        @cache_data(ttl=ttl)
        def _query(query: str) -> dict:
            return collection.find_one(query)
        return _query(query)