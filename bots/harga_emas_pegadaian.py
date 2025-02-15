import csv
import datetime
import re
import uuid
import logging
from pathlib import Path
from tempfile import gettempdir

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Setup Selenium options
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--disable-features=NetworkService")
options.add_argument("--window-size=1920x1080")

# Setup Selenium driver
driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 10)

driver.get("https://sahabat.pegadaian.co.id/harga-emas")
wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="__nuxt"]')))

# Scrape process
title = driver.title
logging.info(f"Page: {title}")

try:
    beli_emas_xpath = '//*[@id="__nuxt"]/div/div[2]/main/div[2]/div[2]/div/div/div/div/div/div[1]/div/div[2]/div[2]/div/div/div[1]'
    beli_emas_element = wait.until(EC.presence_of_element_located((By.XPATH, beli_emas_xpath)))
    beli_emas_text = beli_emas_element.text
except Exception as e:
    logging.error(f"Harga Beli: {str(e)}")
    beli_emas_text = None

try:
    jual_emas_xpath = '//*[@id="__nuxt"]/div/div[2]/main/div[2]/div[2]/div/div/div/div/div/div[1]/div/div[2]/div[2]/div/div/div[2]'
    jual_emas_element = wait.until(EC.presence_of_element_located((By.XPATH, jual_emas_xpath)))
    jual_emas_text = jual_emas_element.text
except Exception as e:
    logging.error(f"Harga Jual: {str(e)}")
    jual_emas_text = None

def get_harga(text):
    re_match = re.search(r"Rp\s*([\d\.]+)", text)
    if re_match:
        return re_match.group(1).replace(".", "")

# Save data into CSV file
if beli_emas_text and jual_emas_text:
    new_data = {
        "id": str(uuid.uuid4()),
        "harga_beli": get_harga(beli_emas_text), 
        "harga_jual": get_harga(jual_emas_text), 
        "timestamp": datetime.datetime.now()
    }

    output_file = Path(gettempdir()) / "harga_emas_pegadaian.csv"
    
    with open(output_file, "a", newline="", encoding="utf-8") as f:
        field_names = ["id", "harga_beli", "harga_jual", "timestamp"]
        writer = csv.DictWriter(f, fieldnames=field_names)

        if f.tell() == 0:
            writer.writeheader()

        writer.writerow(new_data)

    logging.info("New row of harga emas added successfully!")

else:
    logging.warning("Pegadaian Harga page failed to scrape")

driver.quit()
