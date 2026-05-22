"""Centralized config and constants. One place to change them."""
import os

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL: str = os.environ["DATABASE_URL"]

SOCRATA_BASE_URL: str = "https://data.cityofchicago.org/resource"
WARDS_DATASET_ID: str = "p293-wvbd"  # Boundaries - Wards (2023-)

HTTP_TIMEOUT_SECONDS: int = 180



# Chicago 311 service requests dataset (post-2018 system).
# This is the dataset the pothole records live in — different from the
# wards dataset (p293-wvbd), which is loaded by load_wards.py.
POTHOLES_DATASET_ID = "v6vf-nfxy"

# Service request short code for "Pothole in Street Complaint".
# Filtering server-side on this is essential — the dataset has millions
# of non-pothole rows we have no interest in.
POTHOLE_SR_SHORT_CODE = "PHF"

# Socrata's default page size is 1000 and the absolute max is 50000.
# We use 1000: small enough to be polite, large enough that backfill is fast.
# Page size also bounds memory use — each page is parsed into Python dicts
# before being passed to the loader.
PAGE_SIZE = 1000

# Sleep between paginated requests during a long backfill, in seconds.
# Not strictly required (Socrata is generous with anonymous traffic) but
# being a good citizen — and gives the city's API time to breathe.
INTER_REQUEST_DELAY_SECONDS = 0.5