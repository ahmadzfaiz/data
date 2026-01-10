import logging
import re
import os
import csv
import uuid
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

TARGET_URL = "https://sahabat.pegadaian.co.id/harga-emas"

class HTMLDownloader:
    """
    Downloads the dynamic HTML content from the Pegadaian website using Selenium.
    This class is strictly for downloading and saving the HTML source after the 
    main dynamic content has loaded, without validating specific price values.
    """
    def __init__(self):
        """Initializes WebDriver and WebDriverWait."""
        logging.info("Initializing Selenium WebDriver.")
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

        # Set page load strategy to avoid waiting for full load
        options.page_load_strategy = 'eager'

        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 60)
        self.driver.implicitly_wait(5)

    def __del__(self):
        """Ensures the driver quits when the object is destroyed."""
        if hasattr(self, 'driver'):
            self.driver.quit()
            logging.info("Selenium WebDriver quit successfully.")

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

            # Wait for dynamic content to render
            time.sleep(5)
            logging.info("Waited 5 seconds for dynamic content to render.")

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


class DataCleaning:
    EXPECTED_PRICE_COUNT = 2

    def __init__(self, html_file):
        self.html_file = html_file

    def get_price_list(self):
        with open(self.html_file, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
        return soup.find_all(string=re.compile("Rp "))

    def run(self):
        raw_data = self.get_price_list()
        cleaned_prices = ["".join(re.findall(r'\d+', item)) for item in raw_data]

        if len(cleaned_prices) < self.EXPECTED_PRICE_COUNT:
            logging.error(f"Expected at least {self.EXPECTED_PRICE_COUNT} prices, found {len(cleaned_prices)}")
            return None

        logging.info(f"Extracted {len(cleaned_prices)} prices: {cleaned_prices[:self.EXPECTED_PRICE_COUNT]}")
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