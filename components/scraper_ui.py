import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO
from components.auth import get_local_storage
import time
from apify_actors import PlatformScraper


localS = get_local_storage()

def show_all_usernames_dialog(platform, handles):
    """Displays all usernames in a dialog with remove buttons."""
    platform_handles = handles.get(platform, [])
    st.write(f"Total usernames: {len(platform_handles)}")

    # Display all usernames with remove buttons inside the dialog
    for i, username in enumerate(platform_handles):
        d_col_name, d_col_remove = st.columns([4, 1])
        with d_col_name:
            # Use markdown for simple list item appearance
            st.markdown(f"- {username}")
        with d_col_remove:
            # Use unique keys for removal buttons inside the dialog
            if st.button("Remove", key=f"remove_dialog_{platform}_{i}", use_container_width=True):
                # Modify the handles dictionary directly
                handles[platform].remove(username)
                localS.setItem("social_handles", handles)
                st.rerun()

def render_scraper_ui(platform):
    """Render the main scraper UI based on the selected platform"""

    st.markdown(f'<h2 class="sub-header">{platform} Data Scraper</h2>', unsafe_allow_html=True)
    is_scrape_user_comments = st.toggle("Scrape Comments", value=True, key=f"scrape_user_comments_{platform}")

    # Initialize session state for scraped data if not present
    if "scraped_data" not in st.session_state:
        st.session_state.scraped_data = {}
    if "scraping" not in st.session_state:
        st.session_state.scraping = False
    if "scraping_platform" not in st.session_state:
         st.session_state.scraping_platform = None

    # Create columns for better layout
    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown("### Add Usernames")

        handles = localS.getItem("social_handles") or {}
        if not isinstance(handles, dict): # Basic check if storage is corrupted/unexpected
            handles = {}

        # Ensure platform exists in handles
        if platform not in handles:
            handles[platform] = []
            localS.setItem("social_handles", handles) # Save initialization

        # Make sure the platform key holds a list
        if not isinstance(handles.get(platform), list):
             handles[platform] = []
             localS.setItem("social_handles", handles) # Correct if needed

        platform_handles = handles.get(platform, []) # Use this list reference


        # Add username input
        username_input = st.text_input(
            f"Enter {platform} username",
            placeholder=f"e.g. {get_example_username(platform)}",
            key=f"username_input_{platform}" # Add key for potential state issues
        )

        col_add, col_clear = st.columns(2)
        with col_add:
            if st.button("Add Username", use_container_width=True, key=f"add_user_{platform}"):
                if username_input:
                    # Check for duplicates *before* adding
                    if username_input in platform_handles:
                        st.warning(f"Username '{username_input}' already exists")
                    else:
                        # Modify the list directly
                        platform_handles.append(username_input)
                        localS.setItem("social_handles", handles) # Save updated handles
                        st.success(f"Added: {username_input}")
                        st.rerun() # Rerun to clear input and update list
                else:
                    st.warning("Please enter a valid username")

        with col_clear:
            if st.button("Clear All", use_container_width=True, key=f"clear_users_{platform}"):
                handles_copy = handles.copy() # Copy to avoid modifying while iterating
                handles_copy[platform] = [] # Clear the list for this platform
                handles[platform] = handles_copy[platform] # Update the original handles
                
                platform_handles = handles_copy[platform] # Update the reference
                localS.deleteItem("social_handles") # Clear the storage
                print(handles_copy)
                localS.setItem("social_handles", handles_copy) # Save cleared list
                print(localS.getItem("social_handles"))

                st.success("All usernames cleared!")
                st.rerun() # Rerun to reflect clearance

        # Display added usernames section
        if platform_handles:
            st.markdown("### Added Usernames")
            display_limit = 5

            # Display the first few usernames directly
            usernames_to_show_directly = platform_handles[:display_limit]
            for i, username in enumerate(usernames_to_show_directly):
                col_name, col_remove = st.columns([3, 1])
                with col_name:
                    st.markdown(f"**{i+1}.** {username}")
                with col_remove:
                    # Key for main list remove button
                    if st.button("Remove", key=f"remove_main_{platform}_{i}", use_container_width=True):
                        # Remove from the list
                        platform_handles.remove(username)
                        localS.setItem("social_handles", handles) # Save changes
                        st.rerun() # Rerun to update the list

            # Add "More..." button if the list exceeds the limit
            if len(platform_handles) > display_limit:
                @st.dialog(f"All {platform} Usernames ({len(platform_handles)})", width="large")
                def display_all_usernames_wrapper():
                    show_all_usernames_dialog(platform, handles)

                # Button to trigger the dialog
                if st.button(f"More... ({len(platform_handles) - display_limit} more)", key=f"show_all_users_button_{platform}", use_container_width=True):
                    display_all_usernames_wrapper() # Call the function decorated with @st.dialog

    with col2:
        st.markdown("### Time Settings")

        # Date range selection
        today = datetime.now().date() # Use .date() for date inputs
        start_date = st.date_input(
            "Start Date",
            today - timedelta(days=7),
            max_value=today, # Prevent selecting future start dates
            help="Select the start date for data collection",
            key=f"start_date_{platform}"
        )

        end_date = st.date_input(
            "End Date",
            today,
            min_value=start_date, # End date cannot be before start date
            max_value=today,      # Prevent selecting future end dates
            help="Select the end date for data collection",
            key=f"end_date_{platform}"
        )

        # Validate date range
        if start_date > end_date:
            st.error("End date must be on or after start date.")
            valid_dates = False
        else:
            valid_dates = True

        # Data limits
        st.markdown("### Data Limits")
        col_posts, col_comments = st.columns(2)
        with col_posts:
            max_posts = st.number_input(
                "Max Posts",
                min_value=1,
                value=200,
                step=50,
                help="Maximum number of posts to scrape per user",
                key=f"max_posts_{platform}"
            )
        with col_comments:
            max_comments = st.number_input(
                "Max Comments",
                min_value=1,
                value=200,
                step=50,
                help="Maximum number of comments to scrape per post",
                key=f"max_comments_{platform}"
            )

        # Output format selection
        st.markdown("### Output Format")
        output_format = st.selectbox(
            "Select output format",
            ["CSV", "JSON", "Excel"],
            index=0,
            help="Choose the file format for the scraped data",
            key=f"output_format_{platform}"
        )

    # Check if there are usernames and dates are valid to scrape
    can_submit = bool(platform_handles) and valid_dates # Check non-empty list and valid dates

    # Start Scraping button
    scrape_button_key = f"start_scraping_{platform}"
    if st.button("Start Scraping", disabled=not can_submit, use_container_width=True, key=scrape_button_key):
        if can_submit: # Double check condition just in case
            st.session_state.scraping = True
            st.session_state.scraping_platform = platform
            st.rerun() # Rerun to trigger the scraping logic below

    # Perform scraping if in progress for this platform
    if st.session_state.get('scraping', False) and st.session_state.get('scraping_platform') == platform:
        # Ensure handles used for scraping are the current ones
        current_platform_handles = localS.getItem("social_handles").get(platform, [])
        if not current_platform_handles:
             st.warning(f"No usernames found for {platform} to scrape. Aborting.")
             st.session_state.scraping = False
             st.session_state.scraping_platform = None
             st.rerun()
        else:
            with st.spinner(f"Scraping {platform} data for {len(current_platform_handles)} user(s)..."):
                try:
                    # Prepare handles for the scraping function
                    user_handles_to_scrape = {platform: current_platform_handles}
                    print(localS.getItem("APIFY_API_KEY"))

                    platform_scraper = PlatformScraper(api_key=localS.getItem("APIFY_API_KEY"))

                    scraped_df_dict = platform_scraper.scrape(
                        platform=platform,
                        start=start_date,  # Pass date objects directly
                        end=end_date,      # Pass date objects directly
                        max_posts=max_posts,
                        max_comments=max_comments,
                        handles=user_handles_to_scrape[platform],
                        scrape_comments=is_scrape_user_comments,
                    )
                    
                    if scraped_df_dict is None:
                        st.error("Invalid API key or Actor ID. Please check your credentials.")
                        st.session_state.scraping = False
                        st.session_state.scraping_platform = None
                        # Add delay to show the error message before state reset
                        time.sleep(20)  # 20 second delay to ensure error message is seen
                        st.session_state.scraping = False
                        st.session_state.scraping_platform = None
                    else:
                        print(scraped_df_dict)  # Debugging output to see what was returned

                        # Process results for the current platform
                        
                        
                        scraped_df = scraped_df_dict['posts']
                        
                        if is_scrape_user_comments:
                            scraped_df = scraped_df_dict['comments']

                        if scraped_df is not None and not scraped_df.empty:
                            st.session_state.scraped_data[platform] = scraped_df
                            st.success(f"Scraped {len(scraped_df)} records for {platform}.")
                        elif scraped_df is not None and scraped_df.empty:
                            st.info(f"Scraping finished, but no data matched the criteria for {platform}.")
                            # Store empty df to indicate scraping happened but found nothing
                            st.session_state.scraped_data[platform] = pd.DataFrame()
                        else:
                            st.error(f"Scraping process did not return data for {platform}.")
                            # Optionally clear scraped data state for this platform if it failed
                            if platform in st.session_state.scraped_data:
                                del st.session_state.scraped_data[platform]

                except Exception as e:
                    import traceback
                    traceback.print_exc()  # Print the full traceback for debugging
                    st.error(f"An error occurred during scraping: {e}")
                    time.sleep(5)  # Delay to allow user to read the error message
                    # Optionally clear scraped data state on error
                    if platform in st.session_state.scraped_data:
                         del st.session_state.scraped_data[platform]

                finally:
                    # Always reset scraping state after attempt
                    st.session_state.scraping = False
                    st.session_state.scraping_platform = None
                    st.rerun() # Rerun to show results/errors and hide spinner
                    
    if any(df is not None and not df.empty for df in st.session_state.scraped_data.values()):
        if st.button("Go to Analytics", key=f"go_to_analytics_from_scraper_{platform}", use_container_width=True):
            st.session_state.current_page = "Analytics"
            st.rerun()

    # Display scraped data if available
    if platform in st.session_state.scraped_data:
        scraped_df = st.session_state.scraped_data[platform]
        if not scraped_df.empty:
            with st.expander(f"{platform} Scraped Data ({len(scraped_df)} rows)", expanded=True):
                st.dataframe(scraped_df)

                # Prepare download
                try:
                    if output_format == "CSV":
                        data = scraped_df.to_csv(index=False).encode('utf-8')
                        mime = 'text/csv'
                        filename = f"{platform}_data_{datetime.now():%Y%m%d_%H%M}.csv"
                    elif output_format == "JSON":
                        data = scraped_df.to_json(orient='records', indent=2).encode('utf-8')
                        mime = 'application/json'
                        filename = f"{platform}_data_{datetime.now():%Y%m%d_%H%M}.json"
                    elif output_format == "Excel":
                        data = scraped_df.to_excel(index=False).encode('utf-8')
                        mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                        filename = f"{platform}_data_{datetime.now():%Y%m%d_%H%M}.xlsx"

                    st.download_button(
                        label=f"Download {platform} data as {output_format}",
                        data=data,
                        file_name=filename,
                        mime=mime,
                        key=f"download_{platform}"
                    )
                except Exception as e:
                    st.error(f"Error preparing download file: {e}")
        elif st.session_state.get('scraping', False) == False: # Only show 'no data' if not currently scraping
             st.info(f"No data was found for {platform} matching the specified criteria.")


def get_example_username(platform):
    """Return example username based on platform"""
    examples = {
        "Twitter": "elonmusk",
        "Facebook": "zuck",
        "Instagram": "instagram"
    }
    return examples.get(platform, "username")