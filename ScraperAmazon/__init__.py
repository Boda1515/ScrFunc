from urllib.parse import urljoin
from bs4 import BeautifulSoup, SoupStrainer
from datetime import datetime
import logging
import aiohttp
import asyncio
import random
import re
import chardet
from concurrent.futures import ThreadPoolExecutor
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class WebScraper:
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36 Edg/93.0.961.47',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36 OPR/79.0.4143.50',
            'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36 Vivaldi/4.1',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/601.7.7 (KHTML, like Gecko) Version/9.1.2 Safari/601.7.7',
        ]
        self.current_user_agent_index = 0
        self.connector = aiohttp.TCPConnector(limit=10, force_close=True)
        self.product_strainer = SoupStrainer(
            ['a'], class_='a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal')
        self.thread_pool = ThreadPoolExecutor(max_workers=4)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(connector=self.connector)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'session'):
            await self.session.close()
        self.thread_pool.shutdown(wait=True)

    def get_next_user_agent(self):
        user_agent = self.user_agents[self.current_user_agent_index]
        self.current_user_agent_index = (
            self.current_user_agent_index + 1) % len(self.user_agents)
        return user_agent

    async def fetch_page(self, url, max_retries=2, retry_delay=2):
        headers = {
            "User-Agent": self.get_next_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
        }

        for attempt in range(max_retries):
            try:
                async with self.session.get(url, headers=headers, timeout=30) as response:
                    if response.status == 403:
                        logging.warning(
                            "CAPTCHA detected via HTTP status 403.")
                        return None

                    try:
                        html = await response.text()
                    except aiohttp.ClientPayloadError:
                        raw_html = await response.read()
                        encoding = chardet.detect(
                            raw_html).get('encoding', 'utf-8')
                        html = raw_html.decode(encoding, errors="replace")

                    logging.info(f"Successfully fetched page from {url[-4:]}")
                    return html

            except asyncio.TimeoutError:
                logging.error(f"Timeout fetching {url}. Retrying...")
            except aiohttp.ClientError as e:
                logging.error(f"Error fetching {url}: {str(e)}")

            await asyncio.sleep(retry_delay)

        logging.error(f"Max retries reached for {url}")
        return None

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

    def process_table(self, table, table_name, product_data):
        """Process a single table in a separate thread"""
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
        return product_data

    async def process_tables_parallel(self, soup, product_data):
        """Process all tables in parallel using thread pool"""
        tables = {
            'first_table': '.a-normal.a-spacing-micro',
            'tech_specs': '#productDetails_techSpec_section_1',
            'right_table': '#productDetails_detailBullets_sections1',
            'new_table': 'ul.a-unordered-list.a-nostyle.a-vertical.a-spacing-none.detail-bullet-list'
        }

        table_tasks = []
        for table_name, selector in tables.items():
            table = soup.select_one(selector)
            if table:
                future = self.thread_pool.submit(
                    self.process_table, table, table_name, product_data.copy())
                table_tasks.append(future)

        if table_tasks:
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                None,
                lambda: [future.result() for future in table_tasks]
            )

            merged_data = product_data.copy()
            for result in results:
                merged_data.update(result)
            return merged_data

        return product_data

    async def scrape_product_data(self, session, product_url, region):
        html = await self.fetch_page(product_url)
        if not html:
            return None
        soup = BeautifulSoup(html, 'html.parser')

        price_selector_1 = "#corePriceDisplay_desktop_feature_div .a-price-whole"
        price_selector_2 = "div.a-section.a-spacing-micro span.a-price.a-text-price.a-size-medium span.a-offscreen"

        price_element = soup.select_one(price_selector_1)
        if price_element:
            price = price_element.get_text(strip=True)
        else:
            price_element = soup.select_one(price_selector_2)
            price = price_element.get_text(
                strip=True) if price_element else None

        discount = None
        possible_selectors = [
            "span.a-color-price",
            ".savingsPercentage"
        ]

        for selector in possible_selectors:
            discount_elements = soup.select(selector)
            for element in discount_elements:
                discount_text = element.get_text(strip=True)
                discount_match = re.search(r'(-?\d+%)', discount_text)
                if discount_match:
                    discount = discount_match.group(1)
                    break
            if discount:
                break

        rate_element = soup.select_one("span.a-icon-alt")
        if rate_element and "out of 5 stars" in rate_element.text:
            rate = rate_element.text.replace("out of 5 stars", "").strip()
        else:
            rate = None

        site = f"amazon_{region.lower()}"
        product_data = {
            "date_column": datetime.today().strftime('%Y-%m-%d'),
            "product_url": product_url,
            "site": site,
            "category": "mobile phones",
            "Title": soup.select_one("#productTitle").text.strip() if soup.select_one("#productTitle") else None,
            "Rate": rate,
            "Price": price,
            "Discount": discount,
            "Image URL": soup.select_one("#imgTagWrapperId img")['src'] if soup.select_one("#imgTagWrapperId img") else None,
            "Description": soup.select_one("#feature-bullets").text.strip() if soup.select_one("#feature-bullets") else None
        }

        product_data = await self.process_tables_parallel(soup, product_data)

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

    async def scrape_page_products(self, page_url, region):
        html = await self.fetch_page(page_url)
        if not html:
            return [], None

        soup = BeautifulSoup(html, 'html.parser',
                             parse_only=self.product_strainer)

        base_urls = {
            'eg': 'https://www.amazon.eg',
            'sa': 'https://www.amazon.sa',
            'us': 'https://www.amazon.com',
            'jp': 'https://www.amazon.co.jp',
            'de': 'https://www.amazon.de',
            'ca': 'https://www.amazon.ca',
            'uk': 'https://www.amazon.co.uk',
            'au': 'https://www.amazon.com.au',
            'ae': 'https://www.amazon.ae',
            'in': 'https://www.amazon.in'
        }

        base_url = base_urls.get(region)
        if not base_url:
            raise ValueError(f"Unsupported region: {region}")

        product_links = {
            urljoin(base_url, link.get('href'))
            for link in soup.find_all('a')
            if link.get('href')
        }

        soup = BeautifulSoup(html, 'html.parser')
        next_button = soup.select_one("a.s-pagination-next")
        next_page_url = urljoin(
            base_url, next_button['href']) if next_button and next_button.get('href') else None

        return list(product_links), next_page_url

    async def scrape_all_products(self, start_page_url, region, max_pages=17):
        all_product_links = set()
        current_page_url = start_page_url
        page_number = 1
        pages_scraped = 0

        sem = asyncio.Semaphore(5)

        async def fetch_product_with_semaphore(url):
            async with sem:
                return await self.scrape_product_data(self.session, url, region)

        while current_page_url and page_number <= max_pages:
            logging.info(f"Scraping page {page_number}: {current_page_url}")
            products, next_page = await self.scrape_page_products(current_page_url, region)

            if products:
                all_product_links.update(products)
                logging.info(
                    f"Found {len(products)} product links on page {page_number}.")
            else:
                logging.info(
                    f"No products found on page {page_number}. Retrying...")
                await asyncio.sleep(5)
                continue

            pages_scraped += 1

            if pages_scraped >= 10:
                random_delay = random.uniform(5, 9)
                logging.info(
                    f"Pausing for {random_delay:.2f} seconds after scraping {pages_scraped} pages.")
                await asyncio.sleep(random_delay)
                pages_scraped = 0

            current_page_url = next_page
            page_number += 1
            await asyncio.sleep(random.uniform(2, 5))

        logging.info(f"Total product links found: {len(all_product_links)}")

        tasks = [fetch_product_with_semaphore(
            url) for url in all_product_links]
        all_product_data = [data for data in await asyncio.gather(*tasks) if data is not None]
        return all_product_data


async def main(input: dict) -> dict:
    try:
        start_url = input['start_url']
        region = input["region"]
        max_pages = int(input["max_pages"]) if isinstance(
            input["max_pages"], str) else input["max_pages"]

        start_time = time.time()
        async with WebScraper() as scraper:
            product_data = await scraper.scrape_all_products(start_url, region, max_pages)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info(f"Time: {elapsed_time}")
        return {
            "region": region,
            "scraped_data": product_data
        }
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return {}
