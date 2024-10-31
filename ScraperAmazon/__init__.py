# ScraperAmazon
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup, SoupStrainer
from datetime import datetime
import logging
import aiohttp
import asyncio
import random
import re
import chardet
import time


class WebScraperImproved:
    def __init__(self, config=None):
        """
        Initialize the web scraper with enhanced configuration and error handling
        """
        # Load configuration
        self.config = config or {
            'max_retries': 3,
            'retry_delay': 2,
            'session_timeout': 30,
            'max_concurrent_requests': 10,
            'max_pages_before_pause': 10,
            'pause_duration': (5, 9),
            'request_delay': (2, 5),
            'required_fields': ['Title', 'Price']
        }

        # Initialize user agents
        self.user_agents = [
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36 Edg/93.0.961.47',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36 OPR/79.0.4143.50',
            'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36 Vivaldi/4.1'
        ]
        self.current_user_agent_index = 0

        # Initialize connections and rate limiting
        self.connector = aiohttp.TCPConnector(
            limit=self.config['max_concurrent_requests'],
            force_close=True
        )
        self.rate_limiter = asyncio.Semaphore(
            self.config['max_concurrent_requests'])
        self.last_request_time = {}

        # Initialize parsers
        self.product_strainer = SoupStrainer(
            ['a'], class_='a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal'
        )

        # Initialize scraped URLs set
        self.scraped_urls = set()

    async def fetch_page(self, url):
        headers = {
            "User-Agent": self.get_next_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        domain = urlparse(url).netloc
        if domain in self.last_request_time:
            elapsed = time.time() - self.last_request_time[domain]
            if elapsed < 1.0:
                await asyncio.sleep(1.0 - elapsed)

        async with self.rate_limiter:
            for attempt in range(self.config['max_retries']):
                try:
                    async with self.session.get(url, headers=headers) as response:
                        if response.status == 403:
                            return await self._handle_rate_limit(url)
                        elif response.status >= 500:
                            return await self._handle_server_error(url)

                        try:
                            html = await response.text()
                        except aiohttp.ClientPayloadError:
                            raw_html = await response.read()
                            encoding = chardet.detect(
                                raw_html).get('encoding', 'utf-8')
                            html = raw_html.decode(encoding, errors="replace")

                        self.last_request_time[domain] = time.time()
                        logging.info(f"Successfully fetched page: {url}")
                        return html

                except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                    delay = (2 ** attempt) * self.config['retry_delay']
                    logging.error(
                        f"Error fetching {url}: {str(e)}. Retrying in {delay}s...")
                    await asyncio.sleep(delay)

            logging.error(f"Max retries reached for {url}")
            return None

    async def __aenter__(self):
        """Set up async context manager"""
        timeout = aiohttp.ClientTimeout(total=self.config['session_timeout'])
        self.session = aiohttp.ClientSession(
            connector=self.connector,
            timeout=timeout
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up async context manager"""
        if hasattr(self, 'session'):
            await self.session.close()

    def get_next_user_agent(self):
        """Get next user agent using round robin"""
        user_agent = self.user_agents[self.current_user_agent_index]
        self.current_user_agent_index = (
            self.current_user_agent_index + 1) % len(self.user_agents)
        return user_agent

    async def _handle_rate_limit(self, url):
        """Handle rate limiting with exponential backoff"""
        delay = random.uniform(30, 60)
        logging.warning(
            f"Rate limit detected for {url}. Waiting {delay:.2f} seconds...")
        await asyncio.sleep(delay)
        return None

    async def _handle_server_error(self, url):
        """Handle server errors"""
        logging.error(f"Server error for {url}")
        return None

    async def scrape_page_products(self, page_url, region):
        """Scrape all product URLs from a single page"""
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
            if link.get('href') and not link.get('href').startswith('#')
        }

        # Get next page URL
        soup = BeautifulSoup(html, 'html.parser')
        next_button = soup.select_one("a.s-pagination-next")
        next_page_url = urljoin(
            base_url, next_button['href']
        ) if next_button and next_button.get('href') else None

        return list(product_links), next_page_url

    def _extract_price(self, soup):
        """Extract price from product page"""
        price_selectors = [
            "#corePriceDisplay_desktop_feature_div .a-price-whole",
            "div.a-section.a-spacing-micro span.a-price.a-text-price.a-size-medium span.a-offscreen"
        ]

        for selector in price_selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
        return None

    def _extract_discount(self, soup):
        """Extract discount information"""
        discount_selectors = [
            "span.a-color-price",
            ".savingsPercentage"
        ]

        for selector in discount_selectors:
            elements = soup.select(selector)
            for element in elements:
                discount_text = element.get_text(strip=True)
                match = re.search(r'(-?\d+%)', discount_text)
                if match:
                    return match.group(1)
        return None

    def _extract_image_url(self, soup):
        """Extract product image URL"""
        img_element = soup.select_one("#imgTagWrapperId img")
        return img_element['src'] if img_element else None

    def _extract_description(self, soup):
        """Extract product description"""
        description_element = soup.select_one("#feature-bullets")
        return description_element.text.strip() if description_element else None

    def _extract_reviews(self, soup):
        """Extract product reviews"""
        reviews = []
        review_cards = soup.select("div[data-hook='review']")

        for review in review_cards[:5]:
            try:
                reviewer_name = review.select_one(
                    "span.a-profile-name").text.strip()
                review_rating = review.select_one(
                    "i.a-icon-star span.a-icon-alt"
                ).text.strip().replace("out of 5 stars", "")
                review_date = review.select_one(
                    "span.review-date").text.strip()
                review_text = review.select_one(
                    "span[data-hook='review-body']"
                ).text.strip()

                reviews.append({
                    "Reviewer": reviewer_name,
                    "Rating": review_rating,
                    "Date": review_date,
                    "Review": review_text
                })
            except Exception as e:
                logging.error(f"Error extracting review: {str(e)}")
                continue

        return reviews

    def _validate_product_data(self, data):
        """Validate product data has required fields"""
        if not all(field in data for field in self.config['required_fields']):
            logging.warning(
                f"Missing required fields for product: {data.get('product_url', 'Unknown URL')}")
            return None
        return data

    def clean_text(self, text):
        """Clean text data"""
        text = re.sub(r'[\u200f\u200e]', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def process_table(self, table, table_name, product_data):
        """Process a single table"""
        try:
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
                        product_data[key] = value
            else:
                rows = table.find_all('tr')
                for row in rows:
                    key_element = row.find(['th', 'td'])
                    value_element = row.find_all(
                        'td')[-1] if row.find_all('td') else None
                    if key_element and value_element:
                        key = self.clean_text(key_element.get_text(strip=True))
                        value = self.clean_text(
                            value_element.get_text(strip=True))
                        product_data[key] = value
        except Exception as e:
            logging.error(f"Error processing table {table_name}: {str(e)}")

        return product_data

    async def scrape_product_data(self, url, region):
        """Scrape data for a single product"""
        if url in self.scraped_urls:
            logging.info(f"Skipping already scraped URL: {url}")
            return None

        html = await self.fetch_page(url)
        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')

        try:
            product_data = {
                "date_column": datetime.today().strftime('%Y-%m-%d'),
                "product_url": url,
                "site": f"amazon_{region.lower()}",
                "category": "mobile phones",
                "Title": soup.select_one("#productTitle").text.strip() if soup.select_one("#productTitle") else None,
                "Price": self._extract_price(soup),
                "Discount": self._extract_discount(soup),
                "Image URL": self._extract_image_url(soup),
                "Description": self._extract_description(soup),
                "Reviews": self._extract_reviews(soup)
            }

            # Process tables directly
            tables = {
                'first_table': soup.select_one('.a-normal.a-spacing-micro'),
                'tech_specs': soup.select_one('#productDetails_techSpec_section_1'),
                'right_table': soup.select_one('#productDetails_detailBullets_sections1'),
                'new_table': soup.select_one('ul.a-unordered-list.a-nostyle.a-vertical.a-spacing-none.detail-bullet-list')
            }

            for table_name, table in tables.items():
                if table:
                    product_data = self.process_table(
                        table, table_name, product_data)

            product_data = self._validate_product_data(product_data)

            if product_data:
                self.scraped_urls.add(url)

            return product_data

        except Exception as e:
            logging.error(f"Error scraping product {url}: {str(e)}")
            return None

    async def scrape_all_products(self, start_page_url, region, max_pages=17):
        """Scrape all products from multiple pages"""
        all_product_links = set()
        current_page_url = start_page_url
        page_number = 1
        pages_scraped = 0

        # Create semaphore for concurrent requests
        sem = asyncio.Semaphore(self.config['max_concurrent_requests'])

        async def fetch_product_with_semaphore(url):
            """Fetch product data with semaphore control"""
            async with sem:
                return await self.scrape_product_data(url, region)

        while current_page_url and page_number <= max_pages:
            logging.info(f"Scraping page {page_number}: {current_page_url}")

            try:
                products, next_page = await self.scrape_page_products(current_page_url, region)

                if products:
                    all_product_links.update(products)
                    logging.info(
                        f"Found {len(products)} product links on page {page_number}. "
                        f"Total unique products: {len(all_product_links)}"
                    )
                else:
                    logging.warning(
                        f"No products found on page {page_number}. Retrying..."
                    )
                    await asyncio.sleep(self.config['retry_delay'])
                    continue

                pages_scraped += 1

                # Implement pause after scraping configured number of pages
                if pages_scraped >= self.config['max_pages_before_pause']:
                    pause_duration = random.uniform(
                        *self.config['pause_duration']
                    )
                    logging.info(
                        f"Pausing for {pause_duration:.2f} seconds after "
                        f"scraping {pages_scraped} pages."
                    )
                    await asyncio.sleep(pause_duration)
                    pages_scraped = 0

                current_page_url = next_page
                page_number += 1

                # Random delay between pages
                await asyncio.sleep(
                    random.uniform(*self.config['request_delay'])
                )

            except Exception as e:
                logging.error(f"Error scraping page {page_number}: {str(e)}")
                break

        logging.info(
            f"Total unique product links found: {len(all_product_links)}")

        # Scrape individual products concurrently
        tasks = [
            fetch_product_with_semaphore(url)
            for url in all_product_links
        ]
        all_product_data = [
            data for data in await asyncio.gather(*tasks)
            if data is not None
        ]

        return all_product_data


async def main(input: dict) -> dict:
    """Process the scraper request and return results"""
    start_time = time.time()

    try:
        # Validate input data
        required_fields = ['start_url', 'region', 'max_pages']
        if not all(field in input for field in required_fields):
            raise ValueError(
                f"Missing required input fields. Required: {required_fields}"
            )

        start_url = input['start_url']
        region = input['region']
        max_pages = int(input['max_pages']) if isinstance(
            input['max_pages'], str
        ) else input['max_pages']

        # Initialize and run scraper
        async with WebScraperImproved() as scraper:
            product_data = await scraper.scrape_all_products(
                start_url, region, max_pages
            )

            end_time = time.time()
            execution_time = end_time - start_time

            result = {
                "status": "success",
                "region": region,
                "total_products": len(product_data),
                "execution_time": f"{execution_time:.2f} seconds",
                "scraped_data": product_data
            }

            logging.info(
                f"Scraping completed in {execution_time:.2f} seconds. "
                f"Found {len(product_data)} products."
            )

            return result
    except Exception as e:
        logging.error(f"An error occurred in main execution: {str(e)}")
        return {
            "status": "error",
            "error": str(e),
            "execution_time": f"{time.time() - start_time:.2f} seconds"
        }
