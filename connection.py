import streamlit as st
from streamlit.connections import ExperimentalBaseConnection
from streamlit.runtime.caching import cache_data

from pymongo import MongoClient
from pymongo.server_api import ServerApi
from pymongo.collection import Collection

class MongoDBConnection(ExperimentalBaseConnection[MongoClient]):

    def __init__(self, db_name: str, collection_name: str, **kwargs):
        super().__init__(**kwargs)
        self.db_name = db_name
        self.collection_name = collection_name

    def _connect(self, **kwargs) -> MongoClient:
        if "CLUSTER_ADDRESS" in kwargs:
            MONGODB_USERNAME = kwargs.pop("MONGODB_USERNAME")
            MONGODB_PASSWORD = kwargs.pop("MONGODB_PASSWORD")
            CLUSTER_ADDRESS = kwargs.pop("CLUSTER_ADDRESS")
        else:
            MONGODB_USERNAME = st.secrets.get("MONGODB_USERNAME")
            MONGODB_PASSWORD = st.secrets.get("MONGODB_PASSWORD")
            CLUSTER_ADDRESS = st.secrets.get("CLUSTER_ADDRESS")
        CONNECTION_STRING = f"mongodb+srv://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@{CLUSTER_ADDRESS}/?retryWrites=true&w=majority"
        return MongoClient(CONNECTION_STRING, server_api=ServerApi('1'))

    def cursor(self) -> Collection:
        db = self._instance[self.db_name]
        return db[self.collection_name]

    def query(self, query, ttl = 3600) -> dict:
        collection = self.cursor()
        @cache_data(ttl=ttl)
        def _query(query: str) -> dict:
            return collection.find_one(query)
        return _query(query)