import csv
import datetime
import logging
import uuid
from zoneinfo import ZoneInfo
from typing import Optional

import requests

UBS_URL = "https://ubslifestyle.com/wp-admin/admin-ajax.php"
AVAILABLE_INTERVALS = [7, 30, 90, 180, 365, 1095]


def build_payload(interval: int = 7) -> dict:
    return {
        "action": "get_harga_emas_hari_ini",
        "path": f"ajax/chart_interval_jual/GOLD/{interval}",
    }


def select_interval(start_date: datetime.date, end_date: datetime.date) -> int:
    """Select the smallest available interval that covers the date range."""
    days_diff = (end_date - start_date).days + 1
    for interval in AVAILABLE_INTERVALS:
        if interval >= days_diff:
            return interval
    return AVAILABLE_INTERVALS[-1]


class DataFetching:
    """Fetches gold price data from UBS endpoint."""

    def __init__(self, interval: int = 7, url: str = UBS_URL):
        self.url = url
        self.payload = build_payload(interval)

    def run(self) -> Optional[list]:
        """Fetch data from UBS API and return the JSON response."""
        logging.info(f"Fetching harga emas UBS with path: {self.payload['path']}")
        try:
            response = requests.post(self.url, data=self.payload)
            response.raise_for_status()
            data = response.json()
            logging.info("Data fetched successfully.")
            return data
        except requests.RequestException as e:
            logging.error(f"Failed to fetch data: {e}")
            return None


class DataCleaning:
    """Parses and validates UBS gold price data."""

    def __init__(
        self,
        data: list,
        target_date: Optional[datetime.date] = None,
        start_date: Optional[datetime.date] = None,
        end_date: Optional[datetime.date] = None,
    ):
        self.data = data
        self.target_date = target_date
        self.start_date = start_date
        self.end_date = end_date

    @staticmethod
    def _parse_entry(entry: list) -> dict:
        """Parse a single data entry [timestamp_ms, open, high, low, close]."""
        entry_date = datetime.datetime.fromtimestamp(
            entry[0] / 1000, tz=ZoneInfo("Asia/Jakarta")
        ).date()
        return {
            "price": entry[4],
            "date": str(entry_date),
        }

    def _run_single(self) -> Optional[dict]:
        """Parse and validate for a single target date."""
        target = self.target_date or datetime.datetime.now(ZoneInfo("Asia/Jakarta")).date()
        latest_entry = self.data[0]["data"][-1]
        entry_date = datetime.datetime.fromtimestamp(
            latest_entry[0] / 1000, tz=ZoneInfo("Asia/Jakarta")
        ).date()

        if entry_date != target:
            logging.warning(
                f"Latest entry date ({entry_date}) is not {target}, skipping."
            )
            return None

        result = self._parse_entry(latest_entry)
        logging.info(f"Harga Buyback: {result['price']}")
        return result

    def _run_bulk(self) -> Optional[list[dict]]:
        """Parse and filter entries within the start_date to end_date range."""
        results = []
        for entry in self.data[0]["data"]:
            entry_date = datetime.datetime.fromtimestamp(
                entry[0] / 1000, tz=ZoneInfo("Asia/Jakarta")
            ).date()
            if self.start_date <= entry_date <= self.end_date:
                results.append(self._parse_entry(entry))

        if not results:
            logging.warning(
                f"No entries found between {self.start_date} and {self.end_date}."
            )
            return None

        logging.info(f"Found {len(results)} entries between {self.start_date} and {self.end_date}.")
        return results

    def run(self) -> Optional[dict | list[dict]]:
        """
        Response format: [{"name": "GOLD", "data": [[timestamp, open, high, low, close], ...]}]

        Returns a single dict for single-date mode, or a list of dicts for bulk mode.
        """
        if not self.data or len(self.data) == 0 or not self.data[0].get("data"):
            logging.warning("No data received from UBS endpoint.")
            return None

        if self.start_date and self.end_date:
            return self._run_bulk()
        return self._run_single()


class DataStoring:
    """Stores UBS gold price data to CSV."""

    FIELD_NAMES = ["id", "price", "date", "timestamp"]

    def __init__(self, output_file: str, price_data: dict | list[dict]):
        self.output_file = output_file
        self.price_data = price_data

    @staticmethod
    def _build_row(entry: dict) -> dict:
        return {
            "id": str(uuid.uuid4()),
            "price": entry["price"],
            "date": entry["date"],
            "timestamp": datetime.datetime.now(ZoneInfo("Asia/Jakarta")),
        }

    def run(self):
        """Append row(s) to the CSV file."""
        entries = self.price_data if isinstance(self.price_data, list) else [self.price_data]

        with open(self.output_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.FIELD_NAMES)

            if f.tell() == 0:
                writer.writeheader()

            for entry in entries:
                writer.writerow(self._build_row(entry))

        logging.info(f"{len(entries)} row(s) of harga emas UBS added successfully!")
