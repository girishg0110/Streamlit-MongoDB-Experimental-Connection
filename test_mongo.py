from connection import MongoDBConnection
import streamlit as st
import json

conn = st.experimental_connection(name = "mongodb", type = MongoDBConnection, 
                                db_name = "seed-swap", collection_name = "offers")
st.sidebar.title("MongoDB Experimental Connection")
st.sidebar.write(
    '''Try out some queries on this MongoDB collection of seed-swapping offers from local gardeners!
    
    {"offered_seed" : "Basil"}
    {"proposer_lastname" : "Zamboni"}
    {"offered_amt" : "20"}
    '''
)
query_str = st.sidebar.text_input("Enter MongoDB query", '{"offered_seed" : "Basil"}')

st.header("Results")
if st.sidebar.button("Find"):
    query = json.loads(query_str)
    data = conn.query(query)
    st.json(data)

