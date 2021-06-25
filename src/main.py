import argparse
import pandas as pd
import requests
from bs4 import BeautifulSoup
import locale
from joblib import Parallel, delayed
import logging
from pathlib import Path


def get_gcd_single_pair(pair):
    locale.setlocale(locale.LC_ALL, 'en_US.UTF8')
    if len(pair) != 6:
        return None
    org = pair[:3]
    dst = pair[3:]
    url = f"http://www.gcmap.com/mapui?P={org}-{dst}"
    html_content = requests.get(url).text
    soup = BeautifulSoup(html_content, "lxml")
    gcd_table = soup.find("table", attrs={'id': 'mdist'})
    distance_str = gcd_table.tbody.find("td", attrs={'class': 'd'})
    return {'NDOD': pair, 'gcd_mile': locale.atoi(distance_str.text.split(' ')[0])}


if __name__ == '__main__':
    logger = logging.getLogger('gcdCalculatorApp')
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-a", "--arps", help="Provide File Path which has airport pairs in each row without header", default=None, type=Path)
    parser.add_argument("-o", "--out", help="Provide File path of the output gcd file", default = None, type = Path)
    args = parser.parse_args()

    with open(args.arps) as f:
        arp_pairs = f.read().splitlines()

    result = pd.DataFrame(Parallel(n_jobs=-1)(delayed(get_gcd_single_pair)(arp_pair) for arp_pair in arp_pairs))
    result.to_csv(args.out, index=False)




