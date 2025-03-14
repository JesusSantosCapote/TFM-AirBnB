import pycountry
from countryinfo import CountryInfo
import shutil
import requests
import os
import time


def normalize_country(country):
    try:
        country = country.replace("-", " ").title()
        country = pycountry.countries.lookup(country)
        return country.name
    except LookupError:
        return None


def get_continent(country):
    if not country:
        return f"Country not found: {country}"
    try:
        return CountryInfo(country).region()
    except:
        return "Continent not found"
    

def download_file(url, directory, filename=None, retries=3, wait_time=2):
    if not filename:
        filename = url.split("/")[-1]
    
    if ("listings" in filename or "reviews" in filename) and ".gz" not in filename:
        filename = "summary_" + filename

    path = os.path.join(directory, filename)

    for attempt in range(1, retries + 1):
        try:
            print(f"Attempt {attempt} to download {url}...")

            response = requests.get(url, stream=True, timeout=10)
            response.raise_for_status()

            with open(path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            print(f"File downloaded successfully: {path}")
            return path

        except requests.exceptions.RequestException as e:
            print(f"Error downloading file (attempt {attempt}): {e}")
            if attempt < retries:
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

    print(f"Failed to download file after {retries} attempts.")
    return None
  
