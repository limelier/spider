import urllib.request as url_req
from urllib.error import URLError
from typing import List, Tuple
from bs4 import BeautifulSoup
import logging
import config as cfg

logger = logging.getLogger('master:scraping')
logging.basicConfig(level=cfg.logging['base_level'])
logger.setLevel(cfg.logging['script_level'])


class RetryError(Exception):
    pass


def find_countries(retries: int = 5) -> List[Tuple[str, str]]:
    """
    Web scrape Alexa Top Sites for the list of supported countries.

    :return: a list of (country, href) pairs, where the href is the country page's path relative to
    https://www.alexa.com/topsites/
    """
    if retries == 0:
        raise RetryError()
    try:
        response = url_req.urlopen('https://www.alexa.com/topsites/countries')
    except URLError as e:
        logger.debug(e)
        logger.error('Connection failed to Alexa Top Sites, retrying %d more times', retries)
        return find_countries(retries-1)
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


def find_sites(href: str, retries: int = 5) -> List[str]:
    """
    Web scrape Alexa Top Sites for the top 50 sites in a given country.

    :param retries: how many times to retry a connection to the Alexa page
    :param href: the country page's path relative to https://www.alexa.com/topsites/
    :return: a list of URLs for the top 50 sites
    """
    if retries == 0:
        raise RetryError()
    url = 'https://www.alexa.com/topsites/' + href
    try:
        response = url_req.urlopen(url)
    except URLError as e:
        logger.debug(e)
        logger.error('Connection failed to %r, retrying %d more times', url, retries)
        return find_sites(href, retries-1)
    data = response.read()
    soup = BeautifulSoup(data, 'html.parser')
    return ['http://' + div.a.string.lower() for div in soup.select('.DescriptionCell')]
