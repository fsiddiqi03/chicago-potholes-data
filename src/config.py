"""Centralized config and constants. One place to change them."""
import os

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL: str = os.environ["DATABASE_URL"]

SOCRATA_BASE_URL: str = "https://data.cityofchicago.org/resource"
WARDS_DATASET_ID: str = "p293-wvbd"  # Boundaries - Wards (2023-)

HTTP_TIMEOUT_SECONDS: int = 60
