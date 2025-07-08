import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from datetime import datetime
import logging
import os
from fake_useragent import UserAgent

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialise user agent rotator
ua = UserAgent()

def clean_text(text):
    """Clean text by removing extra whitespace and special characters."""
    text = re.sub(r'\s+', ' ', text.strip())
    text = re.sub(r'[^\w\s,.]', '', text)
    return text

def extract_brand(text):
    """Extract charger brand from comment text. List includes home chargepoint brands available in UK and EU as of 2025 and common misspellings."""
    brands = [
        'Hypervolt', 'Hypervault', 'Ohme', 'Zappi', 'Project EV', 'Pod Point', 'Wallbox', 'Easee',
        'Rolec', 'EO Charging', 'Andersen', 'Anderson', 'SyncEV', 'Alfen', 'EVBox', 'ChargePoint', 
        'Tesla', 'ABB', 'Garo', 'NewMotion', 'Shell Recharge', 'Connected Kerb', 'Hive', 'EVEC',
        'Simpson & Partners', 'Simpson', 'PodPoint', 'myenergi', 'myenergy', 'NexBlue', 'GivEnergy', 'Indra'
    ]
    for brand in brands:
        if re.search(rf'\b{brand}\b', text, re.IGNORECASE):
            return brand
    return 'Unknown'

def extract_tariff(text):
    """Extract electricity tariff from comment text."""
    tariffs = [
        'Agile Octopus',
        'Intelligent Octopus',
        'Octopus Go',
        'OVO Charge Anytime',
        'Octopus',
        'OVO',
        'British Gas',
        'IOG',
        'Agile'
    ]
    for tariff in tariffs:
        if re.search(rf'\b{tariff}\b', text, re.IGNORECASE):
            return tariff
    return 'None'

def extract_reason(text, brand):
    """Extract reason for choosing the brand."""
    sentences = text.split('.')
    for sentence in sentences:
        if re.search(rf'\b{brand}\b', sentence, re.IGNORECASE):
            return clean_text(sentence)
    return clean_text(text[:200])  # Fallback to first 200 chars

def is_reddit_url(url):
    """Check if the URL is a Reddit URL."""
    return 'reddit.com' in url.lower()

def convert_to_old_reddit(url):
    """Convert Reddit URL to old.reddit.com format."""
    if is_reddit_url(url):
        url = url.replace('www.reddit.com', 'old.reddit.com')
        if not url.startswith('https://old.reddit.com'):
            url = url.replace('reddit.com', 'old.reddit.com')
    return url

def scrape_reddit_thread(url, max_retries=1):
    """Scrape all loaded comments from a Reddit thread on old.reddit.com."""
    data = []
    try:
        url = convert_to_old_reddit(url)
        logger.info(f"Fetching Reddit thread: {url}")
        headers = {'User-Agent': ua.random}
        for attempt in range(max_retries + 1):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                break
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt == max_retries:
                    logger.error(f"Failed to fetch {url} after {max_retries + 1} attempts")
                    return data
                time.sleep(5)  # Wait before retrying

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Check for comments container
        comment_area = soup.find('div', class_='commentarea')
        if not comment_area:
            logger.error(f"No comment area found in {url}. Page may be inaccessible or empty.")
            return data

        # Find thread title
        title = soup.find('a', class_='title')
        thread_title = clean_text(title.text) if title else 'Unknown Title'
        print(f"\nProcessing Reddit thread: {thread_title}")

        # Find all loaded comments
        comments = soup.find_all('div', class_='comment')
        logger.info(f"Found {len(comments)} loaded comments in {url}")
        
        if not comments:
            logger.warning(f"No comments found in {url}. Check HTML structure or comment availability.")

        for idx, comment in enumerate(comments):
            try:
                # Get comment text
                text_div = comment.find('div', class_='usertext-body')
                comment_text = text_div.get_text(strip=True) if text_div else ''
                if not comment_text:
                    logger.debug(f"Skipping comment {idx}: Empty text")
                    continue
                if comment_text in ['[deleted]', '[removed]']:
                    logger.debug(f"Skipping comment {idx}: Deleted or removed")
                    continue

                brand = extract_brand(comment_text)
                if brand == 'Unknown':
                    logger.debug(f"Skipping comment {idx}: No brand found")
                    continue

                reason = extract_reason(comment_text, brand)
                tariff = extract_tariff(comment_text)
                author_tag = comment.find('a', class_='author')
                author = author_tag.text if author_tag else 'Unknown'
                comment_id = comment.get('data-fullname', f'web_{idx}')
                comment_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                comment_data = {
                    'Source': 'Reddit',
                    'Thread_URL': url,
                    'Thread_Title': thread_title,
                    'Comment_ID': comment_id,
                    'Comment_Author': author,
                    'Comment_Date': comment_date,
                    'Brand': brand,
                    'Reason': reason,
                    'Tariff': tariff,
                    'Comment_Text': clean_text(comment_text)
                }
                data.append(comment_data)
                print(f"Found comment by {author}: Brand={brand}, Tariff={tariff}, Reason={reason}")
            except Exception as e:
                logger.error(f"Error processing Reddit comment {idx} in {url}: {str(e)}")
                continue

    except Exception as e:
        logger.error(f"Error processing Reddit thread {url}: {str(e)}")

    return data

def scrape_forum_thread(url, max_retries=1):
    """Scrape top level posts from generic forum threads."""
    data = []
    try:
        logger.info(f"Fetching forum thread: {url}")
        headers = {'User-Agent': ua.random}
        for attempt in range(max_retries + 1):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                break
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt == max_retries:
                    logger.error(f"Failed to fetch {url} after {max_retries + 1} attempts")
                    return data
                time.sleep(5)

        soup = BeautifulSoup(response.text, 'html.parser')

        # Try to find thread title
        title = soup.find('h1') or soup.find('title')
        thread_title = clean_text(title.text) if title else 'Unknown Title'
        print(f"\nProcessing forum thread: {thread_title}")

        # Look for common forum post containers
        possible_post_selectors = [
            'div.post', 'article', 'div.message', 'div.post-body', 
            'div.forum-post', 'div.comment'
        ]
        posts = []
        for selector in possible_post_selectors:
            posts = soup.select(selector)
            if posts:
                logger.info(f"Found {len(posts)} posts using selector {selector}")
                break

        for idx, post in enumerate(posts):
            try:
                post_text = post.get_text(strip=True)
                if not post_text:
                    logger.debug(f"Skipping forum post {idx}: Empty text")
                    continue

                brand = extract_brand(post_text)
                if brand == 'Unknown':
                    logger.debug(f"Skipping forum post {idx}: No brand found")
                    continue

                reason = extract_reason(post_text, brand)
                tariff = extract_tariff(post_text)
                author_tag = (post.find('a', class_='username') or 
                            post.find('span', class_='author') or 
                            post.find('div', class_='author'))
                author = author_tag.text if author_tag else 'Unknown'
                comment_id = f'forum_{idx}'
                comment_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                post_data = {
                    'Source': 'Forum',
                    'Thread_URL': url,
                    'Thread_Title': thread_title,
                    'Comment_ID': comment_id,
                    'Comment_Author': author,
                    'Comment_Date': comment_date,
                    'Brand': brand,
                    'Reason': reason,
                    'Tariff': tariff,
                    'Comment_Text': clean_text(post_text)
                }
                data.append(post_data)
                print(f"Found forum post by {author}: Brand={brand}, Tariff={tariff}, Reason={reason}")
            except Exception as e:
                logger.error(f"Error processing forum post {idx} in {url}: {str(e)}")
                continue

    except Exception as e:
        logger.error(f"Error processing forum thread {url}: {str(e)}")

    return data

def read_urls_from_file(filename):
    """Read URLs from a .txt file."""
    if not os.path.exists(filename):
        logger.error(f"URL file {filename} not found.")
        return []
    with open(filename, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]
    return urls

def save_to_csv(data, filename='ev_charger_data.csv'):
    """Save scraped data to CSV."""
    if not data:
        logger.warning("No data to save.")
        return
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, encoding='utf-8')
    logger.info(f"Data saved to {filename}")

def main():
    # Read URLs from file
    url_file = 'urls.txt'
    urls = read_urls_from_file(url_file)
    if not urls:
        print("No URLs to process. Please create urls.txt with one URL per line.")
        return

    all_data = []
    for idx, url in enumerate(urls):
        print(f"\nProcessing URL {idx + 1}/{len(urls)}: {url}")
        if is_reddit_url(url):
            data = scrape_reddit_thread(url)
        else:
            data = scrape_forum_thread(url)
        all_data.extend(data)
        time.sleep(2)  # 2-second delay between requests

    # Save to CSV
    save_to_csv(all_data)
    print(f"\nScraping complete. Data saved to ev_charger_data.csv with {len(all_data)} entries.")

if __name__ == "__main__":
    main()