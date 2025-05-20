import streamlit as st
from components.analytics_ui import render_analytics_ui
from components.scraper_ui import render_scraper_ui


def render_ui(platform):
    if 'current_page' not in st.session_state:
        st.session_state.current_page = "Scraper"
        
    # Render content based on the current page state
    if st.session_state.current_page == "Scraper":
        # Select platform for scraping remains on the scraper page
        st.sidebar.title("Scraper Settings") # Optionally add a sidebar title for context
        render_scraper_ui(platform)

    elif st.session_state.current_page == "Analytics":
        render_analytics_ui()
        
    st.markdown("""
    <style>
        .main-header {
            color: #0E1117; /* Streamlit dark text color */
            text-align: center;
            font-size: 2.5em;
            margin-bottom: 0.5em;
        }
        .sub-header {
            color: #0E1117; /* Streamlit dark text color */
            font-size: 1.8em;
            margin-top: 1em;
            margin-bottom: 0.5em;
        }
    </style>
    """, unsafe_allow_html=True)