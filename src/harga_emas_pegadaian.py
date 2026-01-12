import logging
import re
import os
import csv
import uuid
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional
from pathlib import Path

import yaml
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

TARGET_URL = "https://sahabat.pegadaian.co.id/harga-emas"
PROXY_FILE = Path(__file__).parent / "proxies" / "2026-01.yaml"

class HTMLDownloader:
    """
    Downloads the dynamic HTML content from the Pegadaian website using Selenium.
    This class is strictly for downloading and saving the HTML source after the
    main dynamic content has loaded, without validating specific price values.
    Supports proxy rotation for resilient scraping.
    """
    def __init__(self, proxy: Optional[str] = None):
        """Initializes WebDriver and WebDriverWait with optional proxy."""
        self.proxy = proxy
        self.driver = None
        self._init_driver()

    def _init_driver(self):
        """Initialize the Chrome WebDriver with current settings."""
        logging.info(f"Initializing Selenium WebDriver{f' with proxy {self.proxy}' if self.proxy else ''}.")
        options = Options()
        # Use new headless mode (Chrome 109+)
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-extensions")

        # --- ANTI-BOT DETECTION MEASURES ---
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        # Add proxy if provided
        if self.proxy:
            options.add_argument(f"--proxy-server={self.proxy}")

        # Set page load strategy to avoid waiting for full load
        options.page_load_strategy = 'eager'

        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 60)
        self.driver.implicitly_wait(5)

    def quit_driver(self):
        """Quit the current driver."""
        if self.driver:
            try:
                self.driver.quit()
                logging.info("Selenium WebDriver quit successfully.")
            except Exception:
                pass
            self.driver = None

    def __del__(self):
        """Ensures the driver quits when the object is destroyed."""
        self.quit_driver()

    @staticmethod
    def load_proxies(proxy_file: Path = PROXY_FILE) -> list[dict]:
        """Load proxies from YAML file, sorted by uptime (highest first)."""
        if not proxy_file.exists():
            logging.warning(f"Proxy file not found: {proxy_file}")
            return []

        with open(proxy_file, "r") as f:
            data = yaml.safe_load(f)

        proxies = data.get("proxies", [])
        # Sort by uptime percentage (highest first)
        proxies.sort(key=lambda x: int(x.get("uptime", "0%").rstrip("%")), reverse=True)
        return proxies

    @staticmethod
    def format_proxy(proxy_dict: dict) -> str:
        """Format proxy dict to proxy string."""
        return f"{proxy_dict['ip']}:{proxy_dict['port']}"

    def _save_to_html(self, content: str, filename: str) -> str:
        """Saves the provided content to an HTML file."""
        try:
            os.makedirs(os.path.dirname(filename) or '.', exist_ok=True)
            with open(filename, "w", encoding="utf-8") as f:
                f.write(content)
            logging.info(f"Successfully saved page source to {filename}")
            return filename
        except Exception as e:
            logging.error(f"Failed to write content to {filename}: {e}")
            return ""

    def run_scraper(self) -> Optional[str]:
        """
        Executes the navigation and wait process, saves HTML, and returns the filename.
        Only waits for the page structure to load, not specific price data.
        """
        try:
            self.driver.get(TARGET_URL)
            logging.info(f"Navigated to: {TARGET_URL}")

            # 1. Wait for Anti-Bot Check to Pass (Essential)
            try:
                self.wait.until_not(EC.title_is("Just a moment..."))
                logging.info("Anti-bot check passed (Title changed).")
            except TimeoutException:
                logging.error("Timeout waiting for anti-bot check to clear. The scraper may be blocked.")
                return None

            # Wait for prices to load by polling page source
            max_wait = 60  # Maximum wait time in seconds
            poll_interval = 2  # Check every 2 seconds
            elapsed = 0

            logging.info("Waiting for price data to load...")
            while elapsed < max_wait:
                page_source = self.driver.page_source
                if "Rp " in page_source:
                    logging.info(f"Price data detected after {elapsed} seconds.")
                    break
                time.sleep(poll_interval)
                elapsed += poll_interval
                logging.info(f"Still waiting for prices... ({elapsed}s elapsed)")
            else:
                logging.warning(f"Price data not detected after {max_wait} seconds. Saving HTML anyway for debugging.")

            page_title = self.driver.title
            logging.info(f"Page Title: {page_title}")

            # Get page source with error handling
            try:
                page_source = self.driver.page_source
            except Exception as e:
                logging.error(f"Failed to get page source: {e}")
                return None

            # Create a timestamped filename
            now_jakarta = datetime.now(ZoneInfo("Asia/Jakarta"))
            timestamp_str = now_jakarta.strftime("%Y%m%d_%H%M%S")
            filename = f"harga_emas_{timestamp_str}.html"

            return self._save_to_html(page_source, filename)

        except Exception as e:
            logging.critical(f"A fatal error occurred during the HTML download process: {e}")
            return None

    @classmethod
    def run_with_proxy_rotation(cls, proxy_file: Path = PROXY_FILE) -> Optional[str]:
        """
        Run scraper with proxy rotation. Tries each proxy until one succeeds.
        Falls back to no proxy if all proxies fail.

        Returns:
            The filename of the saved HTML, or None if all attempts fail.
        """
        proxies = cls.load_proxies(proxy_file)

        if not proxies:
            logging.warning("No proxies available. Running without proxy.")
            downloader = cls()
            try:
                return downloader.run_scraper()
            finally:
                downloader.quit_driver()

        # Try each proxy
        for i, proxy_dict in enumerate(proxies):
            proxy_str = cls.format_proxy(proxy_dict)
            logging.info(f"Attempting proxy {i + 1}/{len(proxies)}: {proxy_str} (uptime: {proxy_dict.get('uptime', 'N/A')})")

            downloader = None
            try:
                downloader = cls(proxy=proxy_str)
                result = downloader.run_scraper()

                if result:
                    logging.info(f"Successfully scraped using proxy: {proxy_str}")
                    return result
                else:
                    logging.warning(f"Proxy {proxy_str} failed to get valid result. Trying next proxy...")

            except WebDriverException as e:
                logging.warning(f"Proxy {proxy_str} failed with WebDriver error: {e}. Trying next proxy...")
            except Exception as e:
                logging.warning(f"Proxy {proxy_str} failed with error: {e}. Trying next proxy...")
            finally:
                if downloader:
                    downloader.quit_driver()

        # All proxies failed, try without proxy as last resort
        logging.warning("All proxies failed. Attempting without proxy as fallback...")
        downloader = cls()
        try:
            return downloader.run_scraper()
        finally:
            downloader.quit_driver()


class DataCleaning:
    EXPECTED_PRICE_COUNT = 2

    def __init__(self, html_file):
        self.html_file = html_file

    def get_price_list(self):
        with open(self.html_file, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
        return soup.find_all(string=re.compile("Rp "))

    def _check_loading_state(self):
        """Check if the page is still in loading state."""
        with open(self.html_file, "r", encoding="utf-8") as f:
            content = f.read()

        loading_indicators = ['loading-spinner', 'skeleton', 'nuxt-loading']
        found_indicators = [ind for ind in loading_indicators if ind.lower() in content.lower()]

        return found_indicators

    def run(self):
        raw_data = self.get_price_list()
        cleaned_prices = ["".join(re.findall(r'\d+', item)) for item in raw_data]

        if len(cleaned_prices) < self.EXPECTED_PRICE_COUNT:
            logging.error(f"Expected at least {self.EXPECTED_PRICE_COUNT} prices, found {len(cleaned_prices)}")

            # Check if page is still loading
            loading_indicators = self._check_loading_state()
            if loading_indicators:
                logging.error(f"Page appears to still be loading. Found indicators: {loading_indicators}")
                logging.error("Consider increasing the wait time for dynamic content to render.")
            else:
                logging.error("No loading indicators found, but prices are missing. Page structure may have changed.")

            return None

        logging.info(f"Prices loaded successfully. Extracted {len(cleaned_prices)} prices: {cleaned_prices[:self.EXPECTED_PRICE_COUNT]}")
        return cleaned_prices
    

class DataStoring:
    def __init__(self, html_file, output_file, price_list):
        self.html_file = html_file
        self.output_file = output_file
        self.price_list = price_list

    def process_new_data(self):
        if not self.price_list or len(self.price_list) < 2:
            raise ValueError(f"Invalid price list: expected at least 2 prices, got {len(self.price_list) if self.price_list else 0}")

        return {
            "id": uuid.uuid4(),
            "harga_beli": self.price_list[0],
            "harga_jual": self.price_list[1],
            "timestamp": datetime.now(ZoneInfo("Asia/Jakarta"))
        }

    def insert_new_data(self):
        with open(self.output_file, "a", newline="", encoding="utf-8") as f:
            field_names = ["id", "harga_beli", "harga_jual", "timestamp"]
            writer = csv.DictWriter(f, fieldnames=field_names)

            if f.tell() == 0:
                writer.writeheader()

            writer.writerow(self.process_new_data())

    def run(self):
        self.insert_new_data()
        os.remove(self.html_file)