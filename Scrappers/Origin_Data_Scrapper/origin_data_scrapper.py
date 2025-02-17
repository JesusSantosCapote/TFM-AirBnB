import requests
from bs4 import BeautifulSoup
import os
from utils import get_continent, normalize_country, download_file
from env import PWD, URL, DESIRED_COUNTRIES


def download_data():
    download_errors = []
    if not os.path.exists(PWD):
        os.makedirs(PWD)

    response = requests.get(URL)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        tds = soup.find_all('td')
        hrefs = []

        for td in tds:
            a_tag = td.find('a')
            if a_tag and 'href' in a_tag.attrs:
                hrefs.append(a_tag['href'])

        for link in hrefs:
            splited_link = link.split('/')
            country = normalize_country(splited_link[3])
            province = splited_link[4]
            city = splited_link[5]

            if country in DESIRED_COUNTRIES:
                continent = get_continent(country)
                final_data_path = os.path.join(PWD, continent, country, province, city)

                if not os.path.exists(final_data_path):
                    os.makedirs(final_data_path)

                down_result = download_file(link, final_data_path)

                if not down_result and (country, city, province) not in download_errors:
                    download_errors.append((country, province, city))

        print()
        print(f"DOWNLOAD ERRORS: {download_errors}")

    else:
        print(response.status_code)


if __name__ == '__main__':
    download_data()