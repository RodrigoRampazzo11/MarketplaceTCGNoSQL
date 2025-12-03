import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import streamlit as st
from dotenv import load_dotenv

if os.getenv('ENV') == 'development':
    load_dotenv()

def init_connection():
    uri = f"mongodb+srv://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}?appName={os.getenv('DB_APPNAME')}"

    try:
        # Create a new client and connect to the server
        client = MongoClient(uri, server_api=ServerApi('1'))
        return client
    except Exception as e:
        st.error(f"Erro na conexão: {e}")
        return None
