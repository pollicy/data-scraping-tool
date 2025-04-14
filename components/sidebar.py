# components/sidebar.py - Sidebar rendering and options

import streamlit as st
from datetime import datetime

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
    
    st.sidebar.markdown("---")
    
    # Add links and information in the sidebar
    st.sidebar.markdown("### Help & Resources")
    st.sidebar.markdown("- [Documentation](https://example.com/docs)")
    st.sidebar.markdown("- [API Reference](https://example.com/api)")
    st.sidebar.markdown("- [Support](mailto:support@example.com)")
    
    return platform