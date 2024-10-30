# ScraperAmazon
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import aiohttp
import asyncio
import random
import re


# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class WebScraper:
    def __init__(self):
        # Define a list of user agents
        self.user_agents = [
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36 Edg/93.0.961.47',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36 OPR/79.0.4143.50',
            'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36 Vivaldi/4.1',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/601.7.7 (KHTML, like Gecko) Version/9.1.2 Safari/601.7.7',
        ]
        self.current_user_agent_index = 0  # Track current user agent index

    def get_next_user_agent(self):
        user_agent = self.user_agents[self.current_user_agent_index]
        self.current_user_agent_index = (
            self.current_user_agent_index + 1) % len(self.user_agents)
        return user_agent

    async def fetch_page(self, session, url, max_retries=2, initial_delay=2):
        user_agent = self.get_next_user_agent()
        headers = {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",  # "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }
        captcha_indicators = ["captcha", "i am not a robot",
                              "robot", "prove you are human", "Enter the characters"]
        delay = initial_delay
        for attempt in range(max_retries):
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 403:
                        logging.warning(
                            "CAPTCHA detected via HTTP status 403.")
                        return None

                    raw_html = await response.read()

                    # Get the encoding from the Content-Type header
                    content_type = response.headers.get('Content-Type', '')
                    encoding = 'utf-8'  # Default fallback
                    if 'charset=' in content_type:
                        encoding = content_type.split('charset=')[-1]

                    # Handle cases where the encoding might not be known
                    if encoding not in ['utf-8', 'ascii', 'latin-1', 'utf-16']:
                        logging.warning(
                            f"Unknown encoding detected: {encoding}. Trying other encodings.")
                        try:
                            html = raw_html.decode('utf-8')
                        except UnicodeDecodeError:
                            try:
                                html = raw_html.decode('latin-1')
                            except UnicodeDecodeError:
                                html = raw_html.decode(
                                    'utf-16', errors='replace')
                    else:
                        html = raw_html.decode(encoding)

                    if any(indicator in str(response.url).lower() for indicator in captcha_indicators):
                        logging.warning("CAPTCHA detected!")
                        return None

                    logging.info(f"Successfully fetched page from {url[-4:]}")
                    return html

            except asyncio.TimeoutError:
                logging.error(
                    f"Timeout error occurred while fetching {url}. Retrying...")
                await asyncio.sleep(delay)
                delay *= 1.5  # Exponential backoff
            except aiohttp.ClientError as e:
                logging.error(f"Error fetching {url}: {str(e)}")
                await asyncio.sleep(delay)
                delay *= 1.5  # Exponential backoff

        logging.error(f"Max retries reached for {url}")
        return None

# This comments because it was not handle japanies regions.
    # async def fetch_page(self, session, url, max_retries=1, initial_delay=2):
    #     user_agent = self.get_next_user_agent()  # Store the updated user agent
    #     headers = {
    #         "User-Agent": user_agent,
    #         "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    #         "Accept-Language": "en-US,en;q=0.5",
    #         "Accept-Encoding": "gzip, deflate, br",
    #         "Connection": "keep-alive",
    #         "Upgrade-Insecure-Requests": "1"
    #     }
    #     captcha_indicators = [
    #         "captcha", "i am not a robot", "robot",
    #         "prove you are human", "Enter the characters"
    #     ]
    #     delay = initial_delay
    #     for attempt in range(max_retries):
    #         try:
    #             async with session.get(url, headers=headers) as response:
    #                 # First check the status code
    #                 if response.status == 403:
    #                     print("CAPTCHA detected via HTTP status 403.")
    #                     return None  # You can handle CAPTCHA solution here if needed
    #                 html = await response.text()  # Directly fetch content

    #                 if any(indicator in str(response.url).lower() for indicator in captcha_indicators):
    #                     print("CAPTCHA detected!")

    #                 logging.info(f"Successfully fetched page from {url[-4:]}")
    #                 return html  # Return the fetched HTML

    #         except asyncio.TimeoutError:
    #             logging.error(
    #                 f"Timeout error occurred while fetching {url}. Retrying...")
    #             await asyncio.sleep(delay)
    #             delay *= 1.5  # Exponential backoff
    #         except aiohttp.ClientError as e:
    #             logging.error(f"Error fetching {url}: {str(e)}")
    #             await asyncio.sleep(delay)
    #             delay *= 1.5  # Exponential backoff

    #     logging.error(f"Max retries reached for {url}")
    #     return None

    def clean_text(self, text):
        text = re.sub(r'[\u200f\u200e]', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def remove_key_from_value(self, key, value):
        key_cleaned = self.clean_text(key)
        value_cleaned = self.clean_text(value)
        if value_cleaned.startswith(key_cleaned):
            return value_cleaned[len(key_cleaned):].strip(" :")
        return value_cleaned

    async def scrape_product_data(self, session, product_url, region):
        html = await self.fetch_page(session, product_url)
        if not html:
            return None
        soup = BeautifulSoup(html, 'html.parser')

        # Extracting Price
        price_selector_1 = "#corePriceDisplay_desktop_feature_div .a-price-whole"
        price_selector_2 = "div.a-section.a-spacing-micro span.a-price.a-text-price.a-size-medium span.a-offscreen"

        price_element = soup.select_one(price_selector_1)
        if price_element:
            price = price_element.get_text(strip=True)
        else:
            price_element = soup.select_one(price_selector_2)
            price = price_element.get_text(
                strip=True) if price_element else None

        # Extract discount
        discount = None

        # Try various selectors
        possible_selectors = [
            "span.a-color-price",
            ".savingsPercentage"
        ]

        for selector in possible_selectors:
            discount_elements = soup.select(selector)
            for element in discount_elements:
                discount_text = element.get_text(strip=True)

                # Flexible regex to capture both negative and non-negative percentages
                discount_match = re.search(r'(-?\d+%)', discount_text)

                if discount_match:
                    discount = discount_match.group(1)
                    break
            if discount:
                break

        # Rating extraction logic
        rate_element = soup.select_one("span.a-icon-alt")
        if rate_element and "out of 5 stars" in rate_element.text:
            rate = rate_element.text.replace("out of 5 stars", "").strip()
        else:
            rate = None
        site = f"amazon_{region.lower()}"
        product_data = {
            "date_column": datetime.today().strftime('%Y-%m-%d'),
            "product_url": product_url,
            "site": site,  # Changed from amazon_sa to amazon_us
            "category": "mobile phones",
            "Title": soup.select_one("#productTitle").text.strip() if soup.select_one("#productTitle") else None,
            "Rate": rate,
            "Price": price,
            "Discount": discount,
            "Image URL": soup.select_one("#imgTagWrapperId img")['src'] if soup.select_one("#imgTagWrapperId img") else None,
            "Description": soup.select_one("#feature-bullets").text.strip() if soup.select_one("#feature-bullets") else None
        }

        tables = {
            'first_table': '.a-normal.a-spacing-micro',
            'tech_specs': '#productDetails_techSpec_section_1',
            'right_table': '#productDetails_detailBullets_sections1',
            'new_table': 'ul.a-unordered-list.a-nostyle.a-vertical.a-spacing-none.detail-bullet-list'
        }

        for table_name, selector in tables.items():
            table = soup.select_one(selector)
            if table:
                if table_name == 'new_table':
                    items = table.find_all('li')
                    for item in items:
                        key_element = item.select_one('span.a-text-bold')
                        value_element = item.find(
                            'span', class_=lambda x: x != 'a-text-bold')
                        if key_element and value_element:
                            key = self.clean_text(
                                key_element.text.strip().replace(':', ''))
                            value = self.clean_text(value_element.text.strip())
                            value = self.remove_key_from_value(key, value)
                            product_data[key] = value
                else:
                    rows = table.find_all('tr')
                    for row in rows:
                        key_element = row.find(['th', 'td'])
                        value_element = row.find_all(
                            'td')[-1] if row.find_all('td') else None
                        if key_element and value_element:
                            key = self.clean_text(
                                key_element.get_text(strip=True))
                            value = self.clean_text(
                                value_element.get_text(strip=True))
                            product_data[key] = value

        reviews = []
        review_cards = soup.select("div[data-hook='review']")
        for review in review_cards[:5]:
            reviewer_name = review.select_one(
                "span.a-profile-name").text.strip()
            review_rating = review.select_one(
                "i.a-icon-star span.a-icon-alt").text.strip().replace("out of 5 stars", "")
            review_date = review.select_one("span.review-date").text.strip()
            review_text = review.select_one(
                "span[data-hook='review-body']").text.strip()
            reviews.append({
                "Reviewer": reviewer_name,
                "Rating": review_rating,
                "Date": review_date,
                "Review": review_text
            })

        product_data['reviews'] = reviews
        return product_data

    async def scrape_page_products(self, session, page_url, region):
        html = await self.fetch_page(session, page_url)
        if not html:
            return [], None

        soup = BeautifulSoup(html, 'html.parser')

        product_links = soup.find_all(
            'a', class_='a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal')
        if region == 'eg':
            base_url = 'https://www.amazon.eg'
        elif region == 'sa':
            base_url = 'https://www.amazon.sa'
        elif region == 'us':
            base_url = 'https://www.amazon.com'
        elif region == 'jp':
            base_url = 'https://www.amazon.co.jp'
        elif region == 'de':  # Germany
            base_url = 'https://www.amazon.de'
        elif region == 'ca':  # Canada
            base_url = 'https://www.amazon.ca'
        elif region == 'uk':  # UK
            base_url = 'https://www.amazon.co.uk'
        elif region == 'au':  # Australia
            base_url = 'https://www.amazon.com.au'
        elif region == 'ae':  # UAE
            base_url = 'https://www.amazon.ae'
        elif region == 'in':  # India
            base_url = 'https://www.amazon.in'
        else:
            raise ValueError(f"Unsupported region: {region}")

        list_products_links = [urljoin(base_url, link.get('href'))
                               for link in product_links if link.get('href')]

        next_button = soup.select_one("a.s-pagination-next")
        next_page_url = urljoin(
            base_url, next_button['href']) if next_button and next_button.get('href') else None

        return list_products_links, next_page_url

    async def scrape_all_products(self, start_page_url, region, max_pages=17):
        all_product_links = set()
        current_page_url = start_page_url
        page_number = 1
        pages_scraped = 0
        async with aiohttp.ClientSession() as session:
            while current_page_url and page_number <= max_pages:
                logging.info(
                    f"Scraping page {page_number}: {current_page_url}")
                products, next_page = await self.scrape_page_products(session, current_page_url, region)

                if products:
                    all_product_links.update(products)
                    logging.info(
                        f"Found {len(products)} product links on page {page_number}.")
                else:
                    logging.info(
                        f"No products found on page {page_number}. Retrying...")
                    # Wait for 5 seconds before retrying
                    await asyncio.sleep(5)
                    continue
                # Increment the pages scraped counter
                pages_scraped += 1

                if pages_scraped >= 10:  # After every 10 pages
                    # Random delay between 10 to 30 seconds
                    random_delay = random.uniform(5, 9)
                    logging.info(
                        f"Pausing for {random_delay: .2f} seconds after scraping {pages_scraped} pages.")
                    await asyncio.sleep(random_delay)
                    pages_scraped = 0  # Reset the counter after the delay

                if not next_page:
                    logging.info("No more pages to scrape.")
                    break

                current_page_url = next_page
                page_number += 1
                await asyncio.sleep(random.uniform(2, 5))

            logging.info(
                f"Total product links found: {len(all_product_links)}")

            tasks = [self.scrape_product_data(
                session, url, region) for url in all_product_links]
            all_product_data = await asyncio.gather(*tasks)
        return all_product_data

# Main function to call from the orchestrator


async def main(input: dict) -> dict:
    # A list of product URLs
    start_url = input['start_url']
    region = input["region"]
    max_pages = input["max_pages"]

    # Convert max_pages to int if it's a string
    max_pages = int(max_pages) if isinstance(max_pages, str) else max_pages

    scraper = WebScraper()

    logging.info(f'Amazon_{region}_product_data function processing...')
    logging.info(f"Number of product links accepted: {len(start_url)}")

    # Scrape products with a time limit of 4,5 minutes
    product_data = await scraper.scrape_all_products(start_url, region, max_pages)

    # Return both the scraped data and any remaining links for the orchestrator
    return {
        "region": region,
        "scraped_data": product_data
    }
