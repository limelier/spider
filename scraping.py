import urllib.request as url_req
from typing import List, Tuple

from bs4 import BeautifulSoup


def find_countries() -> List[Tuple[str, str]]:
    """
    Web scrape Alexa Top Sites for the list of supported countries.

    :return: a list of (country, href) pairs, where the href is the country page's path relative to
    https://www.alexa.com/topsites/
    """
    response = url_req.urlopen('https://www.alexa.com/topsites/countries')  # todo: error handling w/ retries
    data = response.read()
    soup = BeautifulSoup(data, 'html.parser')
    country_lists = soup.find_all('ul', class_='countries', recursive=True)

    country_tuples = []
    for country_list in country_lists:
        country_tuples.extend([
            (country_li.a.string.replace(' ', '_'), country_li.a['href'])
            for country_li in country_list.find_all('li')
        ])
    return country_tuples


def find_sites(href) -> List[str]:
    """
    Web scrape Alexa Top Sites for the top 50 sites in a given country.

    :param href: the country page's path relative to https://www.alexa.com/topsites/
    :return: a list of URLs for the top 50 sites
    """
    response = url_req.urlopen('https://www.alexa.com/topsites/' + href)  # todo: error handling w/ retries
    data = response.read()
    soup = BeautifulSoup(data, 'html.parser')
    return ['http://' + div.a.string.lower() for div in soup.select('.DescriptionCell')]
