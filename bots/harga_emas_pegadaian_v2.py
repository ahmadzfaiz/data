import logging
from src.harga_emas_pegadaian import HTMLDownloader, DataCleaning, DataStoring

# --- Configuration ---
LOG_FILE = "scraper.log"
OUTPUT_FILE = "datasets/harga_emas_pegadaian.csv"
TARGET_URL = "https://sahabat.pegadaian.co.id/harga-emas"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        # FIX: Ensure StreamHandler is correctly referenced from logging
        logging.StreamHandler() 
    ]
)


# STEP 1: Download the HTML using Selenium
html_filename = None
downloader = None
try:
    downloader = HTMLDownloader()
    html_filename = downloader.run_scraper()
except Exception as e:
    logging.critical(f"HTML Downloader failed to run: {e}")
finally:
    del downloader

# STEP 2: Clean the price data
cleaned_data = None
try:
    data_clean = DataCleaning(html_filename)
    cleaned_data = data_clean.run()
except Exception as e:
    logging.critical(f"Harga emas pegadaian data storing failed to run: {e}")
finally:
    del data_clean

# STEP 3: Process the data and store to CSV
try:
    data_store = DataStoring(html_filename, OUTPUT_FILE, cleaned_data)
    data_store.run()
except Exception as e:
    logging.critical(f"Harga emas pegadaian data storing failed to run: {e}")
finally:
    del data_store
