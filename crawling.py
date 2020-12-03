import urllib.request as url_req
from bs4 import BeautifulSoup


def find_countries():
    response = url_req.urlopen('https://www.alexa.com/topsites/countries')
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


def find_sites(country, href):
    response = url_req.urlopen('https://www.alexa.com/topsites/' + href)
    data = response.read()
    soup = BeautifulSoup(data, 'html.parser')
    return ['https://' + div.a.string.lower() for div in soup.select('.DescriptionCell')]
