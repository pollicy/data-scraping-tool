# analytics_utils.py
import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import plotly.express as px
import plotly.graph_objects as go
import io

# Download necessary NLTK data (run this part once or tell the user to run it)
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except Exception as e:
     print("Downloading NLTK vader_lexicon...")
     nltk.download('vader_lexicon')

try:
    nltk.data.find('corpora/stopwords.zip')
except Exception as e:
     print("Downloading NLTK stopwords...")
     nltk.download('stopwords')


# Initialize VADER sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

def perform_sentiment_analysis(df: pd.DataFrame, text_column: str):
    """
    Performs sentiment analysis using VADER on a specified text column and generates a pie chart.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        text_column (str): The name of the column containing the text for analysis.

    Returns:
        tuple: A tuple containing:
               - pd.DataFrame: The original DataFrame with 'vader_score' and 'sentiment' columns added.
               - dict: A dictionary with sentiment counts ('Positive', 'Negative', 'Neutral').
               - plotly.graph_objects.Figure or None: A Plotly pie chart figure showing sentiment distribution, or None if analysis fails or no data.
    """
    if text_column not in df.columns:
        print(f"Error: Text column '{text_column}' not found in DataFrame.")
        return df, {}, None

    # Handle non-string data and missing values gracefully
    df['text_for_sentiment'] = df[text_column].astype(str).fillna('')

    # Apply VADER analyzer
    df['vader_score'] = df['text_for_sentiment'].apply(lambda text: analyzer.polarity_scores(text)['compound'])

    # Categorize sentiment
    df['sentiment'] = df['vader_score'].apply(lambda score: 'Positive' if score >= 0.05 else ('Negative' if score <= -0.05 else 'Neutral'))

    # Calculate counts
    sentiment_counts = df['sentiment'].value_counts().to_dict()

    # --- Generate Pie Chart ---
    sentiment_df = pd.DataFrame(list(sentiment_counts.items()), columns=['Sentiment', 'Count'])

    if sentiment_df.empty or sentiment_df['Count'].sum() == 0:
        print("No sentiment data available to generate pie chart.")
        sentiment_pie_chart = None
    else:
        # Define consistent colors for sentiment
        color_map = {'Positive': 'green', 'Neutral': 'gray', 'Negative': 'red'}
        # Ensure all possible categories are in the DataFrame for consistent coloring
        all_sentiments = ['Positive', 'Neutral', 'Negative']
        sentiment_df = sentiment_df.set_index('Sentiment').reindex(all_sentiments, fill_value=0).reset_index()


        sentiment_pie_chart = px.pie(sentiment_df,
                                     values='Count',
                                     names='Sentiment',
                                     title='Sentiment Distribution',
                                     color='Sentiment', # Use Sentiment column for coloring
                                     color_discrete_map=color_map # Map colors to sentiment values
                                    )
        # Optional: Update layout for better appearance
        sentiment_pie_chart.update_layout(legend_title_text='Sentiment')


    # Clean up temporary column
    df = df.drop(columns=['text_for_sentiment'])

    return df, sentiment_counts, sentiment_pie_chart

def generate_wordcloud(df: pd.DataFrame, text_column: str):
    """
    Generates a word cloud from a specified text column.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        text_column (str): The name of the column containing the text.

    Returns:
        matplotlib.figure.Figure or None: The matplotlib figure containing the word cloud, or None if no text is available.
    """
    if text_column not in df.columns:
        print(f"Error: Text column '{text_column}' not found in DataFrame.")
        return None

    # Combine all text from the specified column, handling NaNs
    text = " ".join(df[text_column].dropna().astype(str).tolist())

    if not text.strip():
        print("No text available to generate wordcloud.")
        return None

    # Basic preprocessing (remove punctuation, lowercase)
    text = text.lower()
    text = ''.join([char for char in text if char.isalnum() or char.isspace()]) # Keep alpha-numeric and space

    # Remove common English stopwords and potentially platform-specific words
    stop_words = set(nltk.corpus.stopwords.words('english'))
    # Add more specific stopwords based on social media context if needed
    social_media_stopwords = set(['rt', 'http', 'https', 'www', 'com']) # Example
    stop_words.update(social_media_stopwords)

    words = text.split()
    filtered_words = [word for word in words if word not in stop_words and len(word) > 1] # Remove single chars

    text = " ".join(filtered_words)

    if not text.strip():
         print("No significant words remaining after cleaning for wordcloud.")
         return None


    # Generate word cloud
    wordcloud = WordCloud(width=800, height=400, background_color='white', collocations=False).generate(text)

    # Plot the word cloud using Matplotlib
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wordcloud, interpolation='bilinear')
    ax.axis("off") # Hide axes
    plt.tight_layout(pad=0)

    return fig

def analyze_trends(df: pd.DataFrame, date_column: str, event_column: str = None, date_granularity='day'):
    """
    Analyzes trends of events over time (e.g., number of posts/comments per day).

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        date_column (str): The name of the column containing date/time information.
        event_column (str, optional): A column to count events by (e.g., 'id'). If None, just counts rows.
        date_granularity (str): The granularity of the time series ('day', 'week', 'month').

    Returns:
        plotly.graph_objects.Figure or None: A Plotly figure showing the trend, or None if date column is missing or invalid.
    """
    if date_column not in df.columns:
        print(f"Error: Date column '{date_column}' not found in DataFrame.")
        return None

    # Ensure date column is datetime objects, handling potential errors
    try:
        df['datetime_col'] = pd.to_datetime(df[date_column], errors='coerce')
    except Exception as e:
        print(f"Error converting date column '{date_column}' to datetime: {e}")
        return None

    # Drop rows where datetime conversion failed
    df_cleaned = df.dropna(subset=['datetime_col']).copy()

    if df_cleaned.empty:
        print("No valid datetime entries found for trend analysis.")
        return None

    # Determine the time grouping format
    if date_granularity == 'day':
        df_cleaned['time_group'] = df_cleaned['datetime_col'].dt.date
    elif date_granularity == 'week':
        # Group by week starting Monday
        df_cleaned['time_group'] = df_cleaned['datetime_col'].dt.to_period('W').dt.start_time.dt.date
    elif date_granularity == 'month':
         df_cleaned['time_group'] = df_cleaned['datetime_col'].dt.to_period('M').dt.start_time.dt.date
    else: # default to day if invalid
         df_cleaned['time_group'] = df_cleaned['datetime_col'].dt.date
         date_granularity = 'day' # Reset granularity


    # Aggregate data
    if event_column and event_column in df_cleaned.columns:
         # Count non-null values in the event column per time group
         trend_data = df_cleaned.groupby('time_group')[event_column].count().reset_index(name='count')
    else:
         # Count rows per time group
         trend_data = df_cleaned.groupby('time_group').size().reset_index(name='count')


    # Sort by date
    trend_data = trend_data.sort_values('time_group')

    if trend_data.empty:
         print("No aggregated data for trend analysis.")
         return None

    # Plot using Plotly Express
    fig = px.line(trend_data, x='time_group', y='count', title=f'Trend Analysis ({date_granularity.capitalize()})')
    fig.update_xaxes(title_text=date_granularity.capitalize())
    fig.update_yaxes(title_text='Count')

    return fig

def analyze_distribution(df: pd.DataFrame, column: str, top_n: int = 10):
    """
    Analyzes the distribution of values in a specified column (e.g., posts per user).

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        column (str): The name of the column to analyze.
        top_n (int): The number of top items to display.

    Returns:
        plotly.graph_objects.Figure or None: A Plotly figure showing the distribution, or None if column is missing.
    """
    if column not in df.columns:
        print(f"Error: Column '{column}' not found in DataFrame.")
        return None

    # Count value occurrences, handle NaNs
    distribution_data = df[column].dropna().value_counts().reset_index()
    distribution_data.columns = [column, 'count']

    if distribution_data.empty:
        print(f"No valid entries found in column '{column}' for distribution analysis.")
        return None

    # Get top N
    distribution_data = distribution_data.head(top_n)

    # Sort for plotting
    distribution_data = distribution_data.sort_values('count', ascending=True)

    # Plot using Plotly Express
    fig = px.bar(distribution_data, x='count', y=column, orientation='h',
                 title=f'Top {top_n} {column} Distribution')
    fig.update_layout(yaxis={'categoryorder':'total ascending'})

    return fig

# Helper to get available text/date columns for the UI
def get_dataframe_columns(df: pd.DataFrame):
    """Returns lists of potential text, date, and categorical columns."""

    text_cols = [col for col in df.columns if df[col].dtype == 'object' or df[col].dtype == 'string']
    # Attempt to identify date columns
    date_cols = [col for col in df.columns if col.lower() in ['date', 'datetime', 'created_at', 'timestamp'] or 'date' in col.lower() or 'time' in col.lower()]

    # Categorical/Distribution columns (often text/object, or int IDs)
    categorical_cols = df.select_dtypes(include=['object', 'string', 'category']).columns.tolist()

    # Remove potential overlaps or less useful columns for distribution
    system_cols = ['vader_score', 'sentiment'] # Columns we might add
    categorical_cols = [col for col in categorical_cols if col not in system_cols and col in df.columns] # Ensure column exists

    # Refine text columns - remove short ID-like columns unless they seem descriptive
    text_cols = [col for col in text_cols if not (df[col].dropna().astype(str).apply(len).mean() < 20 and df[col].nunique() > len(df) * 0.8)] # Heuristic: short average length & high unique count often means IDs
    # Add some common social media text column names if not automatically detected but exist
    common_text_names = ['text', 'caption', 'tweet_content', 'comment_text', 'post_text', 'message']
    for name in common_text_names:
        if name in df.columns and name not in text_cols:
             text_cols.append(name)

    # Refine date columns - ensure they are actually convertible
    valid_date_cols = []
    for col in date_cols:
        try:
            # Check if column is not empty and at least one value can be converted to datetime
            if not df[col].empty and pd.to_datetime(df[col].dropna(), errors='coerce').notna().any():
                 valid_date_cols.append(col)
        except Exception:
            pass # Ignore conversion errors

    # Numeric columns that are not datetime but might be useful
    numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col]) and col not in valid_date_cols]


    # Ensure unique lists and order them
    text_cols = sorted(list(set(text_cols)))
    valid_date_cols = sorted(list(set(valid_date_cols)))
    categorical_cols = sorted(list(set(categorical_cols)))
    numeric_cols = sorted(list(set(numeric_cols)))


    return text_cols, valid_date_cols, categorical_cols, numeric_cols