import streamlit as st
from datetime import datetime
from utils import analytics_utils
import pandas as pd

def render_analytics_ui():
    """Render the analytics UI."""
    col1, col2 = st.columns([4, 1])
    with col1:
        st.markdown('<h1 class="main-header">Social Media Analytics</h1>', unsafe_allow_html=True)
    
    with col2:
        back_button = st.button("Back to Scraper", key="back_to_scraper")
        if back_button:
            st.session_state.current_page = "Scraper"
            st.rerun()

    # Check if any data has been scraped
    if "scraped_data" not in st.session_state or not st.session_state.scraped_data:
        st.info("Please scrape data from the Scraper page first to perform analytics.")
        return

    # Get available platforms from scraped data
    available_platforms = list(st.session_state.scraped_data.keys())
    if not available_platforms:
        st.info("No scraped data available for any platform. Please use the Scraper page.")
        return

    # Select platform
    selected_platform = st.selectbox(
        "Select Platform for Analysis",
        available_platforms,
        key="analytics_platform_select"
    )

    # Get the DataFrame for the selected platform
    platform_df = st.session_state.scraped_data.get(selected_platform)

    if platform_df is None or platform_df.empty:
        st.info(f"No data available for analysis for {selected_platform}.")
        return

    st.write(f"Analyzing data for {selected_platform} ({len(platform_df)} records)")

    # Get potential columns for analysis
    text_cols, date_cols, categorical_cols, numerical_cols = analytics_utils.get_dataframe_columns(platform_df)

    # Use tabs for different analysis types
    tab_sentiment, tab_wordcloud, tab_trends, tab_raw = st.tabs(
        ["Sentiment Analysis", "Word Cloud", "Trends", "Raw Data"]
    )

    with tab_sentiment:
        st.subheader("Sentiment Analysis")
        if not text_cols:
            st.warning("No suitable text columns found for sentiment analysis.")
        else:
            sentiment_col = st.selectbox(
                "Select text column for Sentiment Analysis",
                text_cols,
                key=f"sentiment_col_{selected_platform}"
            )
            if st.button("Run Sentiment Analysis", key=f"run_sentiment_{selected_platform}"):
                 if sentiment_col:
                    with st.spinner("Performing sentiment analysis..."):
                        df_with_sentiment, sentiment_counts, sentiment_plot = analytics_utils.perform_sentiment_analysis(platform_df.copy(), sentiment_col) # Use copy to avoid modifying session state directly unless intended
                        st.write("Sentiment Distribution:")
                        st.dataframe(pd.DataFrame.from_dict(sentiment_counts, orient='index', columns=['Count']))

                        # Display a sample of results
                        st.write("Sample Data with Sentiment Scores:")
                        st.dataframe(df_with_sentiment[[sentiment_col, 'vader_score', 'sentiment']].head())
                        
                        # Display sentiment plot
                        if sentiment_plot:
                            st.plotly_chart(sentiment_plot, use_container_width=True)

                 else:
                    st.warning("Please select a text column.")

    with tab_wordcloud:
        st.subheader("Word Cloud")
        
        if not text_cols:
             st.warning("No suitable text columns found for word cloud generation.")
        else:
            wordcloud_col = st.selectbox(
                "Select text column for Word Cloud",
                text_cols,
                 key=f"wordcloud_col_{selected_platform}"
            )
            if st.button("Generate Word Cloud", key=f"generate_wordcloud_{selected_platform}"):
                if wordcloud_col:
                     with st.spinner("Generating word cloud..."):
                        fig = analytics_utils.generate_wordcloud(platform_df, wordcloud_col)
                        if fig:
                            st.pyplot(fig)
                        else:
                            st.info("Could not generate word cloud. No text available or after cleaning.")
                else:
                    st.warning("Please select a text column.")

    with tab_trends:
        st.subheader("Trend Analysis")
        if not date_cols:
            st.warning("No suitable date/datetime columns found for trend analysis.")
        else:
            trend_date_col = st.selectbox(
                "Select date/datetime column for Trend Analysis",
                date_cols,
                key=f"trend_date_col_{selected_platform}"
            )
            trend_event_col_options = ['(Count of Records)'] + numerical_cols # Allow counting specific events/IDs if available
            trend_event_col_display = st.selectbox(
                 "Count which events?",
                 trend_event_col_options,
                 key=f"trend_event_col_{selected_platform}"
            )
            trend_granularity = st.selectbox(
                "Select time granularity",
                ['day', 'week', 'month'],
                key=f"trend_granularity_{selected_platform}"
            )

            if st.button("Analyze Trends", key=f"analyze_trends_{selected_platform}"):
                if trend_date_col:
                    with st.spinner("Analyzing trends..."):
                        event_col_to_use = trend_event_col_display if trend_event_col_display != '(Count of Records)' else None
                        fig = analytics_utils.analyze_trends(platform_df, trend_date_col, event_column=event_col_to_use, date_granularity=trend_granularity)
                        if fig:
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("Could not analyze trends. Check date column format.")
                else:
                    st.warning("Please select a date column.")

    print("here")
    with tab_raw:
        st.subheader("Raw Scraped Data")
        st.dataframe(platform_df)
        # Option to download raw data again
        try:
             # Using CSV as default for raw download, can add format select if needed
            raw_csv = platform_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label=f"Download Raw {selected_platform} Data (CSV)",
                data=raw_csv,
                file_name=f"{selected_platform}_raw_data_{datetime.now():%Y%m%d_%H%M}.csv",
                mime='text/csv',
                key=f"download_raw_{selected_platform}"
            )
        except Exception as e:
            st.error(f"Error preparing raw data download: {e}")
