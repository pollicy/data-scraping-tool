# components/scraper_ui.py - UI for scraper configuration

import streamlit as st
from datetime import datetime, timedelta
import json
from components.auth import localS
from utils.manage_social_handles import add_social_handle, remove_social_handle, get_social_handles
import time
from apify_actors import scrape_data

def render_scraper_ui(platform):
    """Render the main scraper UI based on the selected platform"""
    
    st.markdown(f'<h2 class="sub-header">{platform} Data Scraper</h2>', unsafe_allow_html=True)
    
    # Create columns for better layout
    col1, col2 = st.columns([3, 2])
    
    with col1:
        st.markdown("### Add Usernames")
        
        # Initialize storage for social handles if not present
        if "social_handles" not in st.session_state:
            st.session_state.social_handles = {}
            # Initialize in local storage if needed - only once per session
            if not localS.getItem("social_handles"):
                localS.setItem("social_handles", {})
            
        # Get current handles from storage
        handles = localS.getItem("social_handles")
        if not handles:
            handles = {}
            
        # Ensure platform exists in handles
        if platform not in handles:
            handles[platform] = []
            localS.setItem("social_handles", handles, key=f"init_{platform}")
            
        # Update session state with current handles
        st.session_state.social_handles = handles
        
        # Add username input
        username_input = st.text_input(
            f"Enter {platform} username",
            placeholder=f"e.g. {get_example_username(platform)}"
        )
        
        col_add, col_clear = st.columns(2)
        with col_add:
            if st.button("Add Username", use_container_width=True):
                if username_input:
                    # Check if username already exists
                    if platform in handles and username_input in handles[platform]:
                        st.warning(f"Username '{username_input}' already exists")
                    else:
                        # Add username to the platform
                        if platform not in handles:
                            handles[platform] = []
                        handles[platform].append(username_input)
                        localS.setItem("social_handles", handles, key=f"add_{platform}_{username_input}")
                        st.session_state.social_handles = handles
                        st.success(f"Added: {username_input}")
                else:
                    st.warning("Please enter a valid username")
        
        with col_clear:
            if st.button("Clear All", use_container_width=True):
                # Clear all usernames for this platform
                if platform in handles:
                    handles[platform] = []
                    localS.setItem("social_handles", handles, key=f"clear_{platform}")
                    st.session_state.social_handles = handles
                    st.success("All usernames cleared!")
        
        # Get platform-specific usernames
        platform_usernames = handles.get(platform, [])
        
        # Display added usernames
        if platform_usernames:
            st.markdown("### Added Usernames")
            for i, username in enumerate(platform_usernames):
                col_name, col_remove = st.columns([3, 1])
                with col_name:
                    st.markdown(f"**{i+1}.** {username}")
                with col_remove:
                    if st.button("Remove", key=f"remove_{platform}_{i}", use_container_width=True):
                        # Remove username
                        handles[platform].remove(username)
                        localS.setItem("social_handles", handles, key=f"remove_{platform}_{username}")
                        st.session_state.social_handles = handles
                        st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown("### Time Settings")
        
        # Date range selection
        today = datetime.now()
        start_date = st.date_input(
            "Start Date",
            today - timedelta(days=7),
            help="Select the start date for data collection"
        )
        
        end_date = st.date_input(
            "End Date",
            today,
            help="Select the end date for data collection"
        )
        
        # New section for data limits
        st.markdown("### Data Limits")
        col_posts, col_comments = st.columns(2)
        with col_posts:
            max_posts = st.number_input(
                "Max Posts",
                min_value=1,
                value=200,
                step=50,
                help="Maximum number of posts to scrape per user"
            )
        with col_comments:
            max_comments = st.number_input(
                "Max Comments",
                min_value=1,
                value=200,
                step=50,
                help="Maximum number of comments to scrape per post"
            )
        
        st.markdown("### Output Format")
        output_format = st.selectbox(
            "Select output format",
            ["CSV", "JSON", "Excel"],
            index=0,
            help="Choose the file format for the scraped data"
        )
        
        st.markdown('</div>', unsafe_allow_html=True)
    
        can_submit = len(platform_usernames) > 0
        
    # Only show the button if not currently scraping
    if not st.session_state.get('scraping', False):
        if st.button("Start Scraping", disabled=not can_submit, use_container_width=True):
            if can_submit:
                # Prepare job configuration
                platform_dict = localS.getItem("social_handles")
                
                scraped_df_dict = scrape_data(
                    start=start_date,
                    end=end_date,
                    max_posts=max_posts,
                    max_comments=max_comments,
                    user_handles=platform_dict
                )
                
                facebook_df = scraped_df_dict.get("facebook")
                instagram_df = scraped_df_dict.get("instagram")
                twitter_df = scraped_df_dict.get("twitter")
                
                print(facebook_df.head())
                print(instagram_df.head())
                print(twitter_df.head())
                st.session_state.scraping = True
                st.session_state.progress = 0
                st.rerun()
            else:
                st.error("Please add at least one username")

    if st.session_state.get('scraping', False):
        st.markdown("### Scraping in progress...")
        
        with st.spinner("Wait for it...", show_time=True):
            time.sleep(5)  # Simulate work
        
        # Reset scraping state when done
        st.session_state.scraping = False
        st.success("Done!")
        st.rerun()  # Refresh to show the button again
                    
    st.markdown('</div>', unsafe_allow_html=True)

def get_example_username(platform):
    """Return example username based on platform"""
    examples = {
        "Twitter": "elonmusk",
        "Facebook": "zuck",
        "Instagram": "instagram"
    }
    return examples.get(platform, "username")

def get_platform_data_types(platform):
    """Return available data types based on platform"""
    data_types = {
        "Twitter": ["Tweets", "Replies", "Media", "Profile Info", "Followers", "Following"],
        "Facebook": ["Posts", "Photos", "Videos", "Events", "Profile Info", "Friends"],
        "Instagram": ["Posts", "Stories", "Reels", "IGTV", "Profile Info", "Followers", "Following"]
    }
    return data_types.get(platform, [])