import csv
import datetime
import re
import uuid
import logging
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# Setup Selenium options
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# Setup Selenium driver
driver = webdriver.Chrome(options=options)
driver.get("https://sahabat.pegadaian.co.id/harga-emas")
driver.implicitly_wait(30)

# Setup logger
logging.basicConfig(
  filename="logs/harga_emas_pegadaian.log",
  level=logging.INFO,
  format='%(asctime)s - %(levelname)s - %(message)s',
  datefmt="%Y-%m-%d %H:%M:%S"
)

# Scrape process
title = driver.title
logging.info(f"Page: {title}")

try:
  beli_emas = '//*[@id="__nuxt"]/div/div[2]/main/div[2]/div[2]/div/div/div/div/div/div[1]/div/div[2]/div[2]/div/div/div[1]'
  beli_emas_element = driver.find_element(By.XPATH, beli_emas)
  beli_emas_text = beli_emas_element.text
except Exception as e:
  logging.error(f"Harga Beli: {str(e)}")
  beli_emas_text = None

try:
  jual_emas = '//*[@id="__nuxt"]/div/div[2]/main/div[2]/div[2]/div/div/div/div/div/div[1]/div/div[2]/div[2]/div/div/div[2]'
  jual_emas_element = driver.find_element(By.XPATH, jual_emas)
  jual_emas_text = jual_emas_element.text
except Exception as e:
  logging.error(f"Harga Jual: {str(e)}")
  jual_emas_text = None

def get_harga(text):
  re_match = re.search(r"Rp\s*([\d\.]+)", text)
  if re_match:
    return re_match.group(1).replace(".", "")

# Add and save data into csv file
if (
  beli_emas_text is not None 
  and jual_emas_text is not None
):
  new_data = {
    "id": uuid.uuid4(),
    "harga_beli": get_harga(beli_emas_text), 
    "harga_jual": get_harga(jual_emas_text), 
    "timestamp": datetime.datetime.now()
  }

  output_file = (
    Path(__file__).parent.parent 
    / "datasets" 
    / "harga_emas_pegadaian.csv"
  )
  with open(output_file, "a", newline="", encoding="utf-8") as f:
    field_names = ["id", "harga_beli", "harga_jual", "timestamp"]
    writer = csv.DictWriter(f, fieldnames=field_names)

    # Only write the header if the file is new/empty
    if f.tell() == 0:
      writer.writeheader()

    writer.writerow(new_data)

  logging.info("New row of harga emas added successfully!")

else:
  logging.warning("Pegadaian Harga page is failed to scrape")

driver.quit()