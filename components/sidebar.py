# components/sidebar.py - Sidebar rendering and options

import streamlit as st
from datetime import datetime
from components.auth import get_local_storage

def render_sidebar():
    """Render sidebar with scraper options""" 
    # Platform selection
    st.sidebar.markdown("### Select Platform")
    platform = st.sidebar.radio(
        "Choose a platform to scrape:",
        ["Twitter", "Facebook", "Instagram"],
        index=0,
        help="Select the social media platform you want to scrape data from"
    )
    
    # Change API key
    localS = get_local_storage()

    api_key = localS.getItem("APIFY_API_KEY")
    
    new_api_key = st.sidebar.text_input(
        "Change API Key",
        value=api_key,
        type="password",
        help="Enter your new APIFY API key to change it"
    )
    if st.sidebar.button("Change API Key"):
        if new_api_key:
            localS.setItem("APIFY_API_KEY", new_api_key, key="change_api_key")
            st.sidebar.success("API Key changed successfully!")
    
    st.sidebar.markdown("---")
    
    # Add links and information in the sidebar
    st.sidebar.markdown("### Help & Resources")
    st.sidebar.markdown("- [Documentation](https://example.com/docs)")
    st.sidebar.markdown("- [API Reference](https://example.com/api)")
    st.sidebar.markdown("- [Support](mailto:support@example.com)")
    
    return platform