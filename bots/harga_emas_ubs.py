import datetime
import logging
import sys
from zoneinfo import ZoneInfo

from src.harga_emas_ubs import DataFetching, DataCleaning, DataStoring, select_interval

# --- Configuration ---
""" Runner
python -m bots.harga_emas_ubs [start_date] [end_date]
"""

LOG_FILE = "scraper_ubs.log"
OUTPUT_FILE = "datasets/harga_emas_ubs.csv"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Determine mode: single date or bulk (start_date + end_date)
if len(sys.argv) > 2:
    # Bulk mode: python bots/harga_emas_ubs.py <start_date> <end_date>
    start_date = datetime.date.fromisoformat(sys.argv[1])
    end_date = datetime.date.fromisoformat(sys.argv[2])
    interval = select_interval(start_date, end_date)
    target_date = None
    logging.info(f"Bulk mode: {start_date} to {end_date} (interval: {interval} days)")
elif len(sys.argv) > 1:
    start_date = None
    end_date = None
    interval = 7
    target_date = datetime.date.fromisoformat(sys.argv[1])
else:
    start_date = None
    end_date = None
    interval = 7
    target_date = datetime.datetime.now(ZoneInfo("Asia/Jakarta")).date()

# STEP 1: Fetch data from UBS API
data = None
try:
    fetcher = DataFetching(interval=interval)
    data = fetcher.run()
except Exception as e:
    logging.critical(f"Data fetching failed: {e}")

if not data:
    logging.critical("Failed to fetch data. Exiting.")
    sys.exit(1)

# STEP 2: Parse and validate the data
price_data = None
try:
    cleaner = DataCleaning(data, target_date=target_date, start_date=start_date, end_date=end_date)
    price_data = cleaner.run()
except Exception as e:
    logging.critical(f"Data cleaning failed: {e}")

if not price_data:
    logging.critical("No valid price data found. Exiting.")
    sys.exit(1)

# STEP 3: Store to CSV
try:
    storer = DataStoring(OUTPUT_FILE, price_data)
    storer.run()
    logging.info("Data successfully stored to CSV.")
except Exception as e:
    logging.critical(f"Data storing failed: {e}")
    sys.exit(1)
