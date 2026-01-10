import logging
import sys
from src.harga_emas_pegadaian import HTMLDownloader, DataCleaning, DataStoring

# --- Configuration ---
LOG_FILE = "scraper.log"
OUTPUT_FILE = "datasets/harga_emas_pegadaian.csv"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)


# STEP 1: Download the HTML using Selenium
html_filename = None
try:
    downloader = HTMLDownloader()
    html_filename = downloader.run_scraper()
except Exception as e:
    logging.critical(f"HTML Downloader failed to run: {e}")

if not html_filename:
    logging.critical("Failed to download HTML. Exiting.")
    sys.exit(1)

# STEP 2: Clean the price data
cleaned_data = None
try:
    data_clean = DataCleaning(html_filename)
    cleaned_data = data_clean.run()
except Exception as e:
    logging.critical(f"Data cleaning failed to run: {e}")

if not cleaned_data:
    logging.critical("Failed to clean data. Exiting.")
    sys.exit(1)

# STEP 3: Process the data and store to CSV
try:
    data_store = DataStoring(html_filename, OUTPUT_FILE, cleaned_data)
    data_store.run()
    logging.info("Data successfully stored to CSV.")
except Exception as e:
    logging.critical(f"Data storing failed to run: {e}")
    sys.exit(1)
