# Social Media Scraper

A Python project that uses the Apify SDK to scrape data from Twitter, Instagram, and Facebook.

## Overview

This tool automates the collection of data from major social media platforms:
- Twitter posts and user information
- Instagram profiles, posts, and comments
- Facebook pages, posts, and engagement metrics

## Requirements

- Python 3.8+
- Apify SDK
- Valid Apify token

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/social-media-scraper.git
cd social-media-scraper

# Set up virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Configuration

1. Create an Apify account at [apify.com](https://apify.com)
2. Get your API token from the Apify Console
3. Paste it into the system, the key is stored in your browser using localstorage so it won't in anyway be access maliciously from the server:

## Usage

![Social Media Scraper Interface](static/interface.png)


1. Run the application:

```bash
python main.py
```

2. The web interface will open in your default browser.

3. Enter your Apify token when prompted.

4. Select the platform and data you wish to scrape:

5. Configure your search parameters and click "Start Scraping".

6. Results will be displayed in the interface and can be exported as CSV or JSON.


## Ethical Considerations

- Always respect website terms of service
- Be mindful of rate limits
- Do not scrape private user information
- Consider user privacy when storing and processing data

## License

MIT

## Disclaimer

This tool is meant for research and educational purposes only. Users are responsible for ensuring their use of this tool complies with applicable laws and platform terms of service.