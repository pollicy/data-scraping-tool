# app.py - Main application file

import streamlit as st
from components.auth import validate, save_api_key
from components.auth import localS
from components.sidebar import render_sidebar
from components.scraper_ui import render_scraper_ui

# Set page configuration
st.set_page_config(
    page_title="Social Media Scraper",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.8rem;
        color: #333;
        margin-bottom: 1rem;
    }
    .card {
        padding: 1.5rem;
        border-radius: 10px;
        background-color: #f8f9fa;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 1rem;
    }
    .footer {
        text-align: center;
        margin-top: 2rem;
        color: #666;
        font-size: 0.8rem;
    }
</style>
""", unsafe_allow_html=True)

def main():
    
    st.markdown('<h1 class="main-header">Social Media Data Scraper</h1>', unsafe_allow_html=True)
    
    # Check if API key exists
    api_key = localS.getItem("APIFY_API_KEY")
    
    if not api_key:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<h2 class="sub-header">Welcome! Please enter your APIFY API Key</h2>', unsafe_allow_html=True)
        api_key_input = st.text_input("API Key", type="password", 
                                      help="Your API key will be securely stored in local storage")
        if st.button("Save API Key", use_container_width=True):
            if api_key_input:
                save_api_key(api_key_input)
                st.success("API Key saved successfully!")
                st.rerun()
            else:
                st.error("Please enter a valid API Key")
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        # Render sidebar with options
        selected_platform = render_sidebar()
        
        # Render main scraper UI based on selected platform
        render_scraper_ui(selected_platform)
        
        st.markdown('<div class="footer">© 2025 Social Media Scraper. All rights reserved.</div>', 
                   unsafe_allow_html=True)

if __name__ == "__main__":
    main()