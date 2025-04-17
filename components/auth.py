# components/auth.py - API key authentication handling

import os
import streamlit as st
from streamlit_local_storage import LocalStorage
localS = LocalStorage()

def validate():
    """Check if API key exists in environment variables"""
    api_key = localS.getItem("APIFY_API_KEY")
    return api_key is not None and api_key != ""

def save_api_key(api_key):
    """Save API key to .env file"""
    if api_key:
        localS.setItem("APIFY_API_KEY", api_key)
        print(f"API Key {api_key} saved to local storage.")
        st.success("API Key saved successfully!")
    else:
        st.error("Please enter a valid API Key.")
        

def get_api_key():
    """Get API key from environment variables"""
    api_key = localS.getItem("APIFY_API_KEY")
    print("APIFY KEY...................................")
    print(api_key)
    return api_key if api_key else None