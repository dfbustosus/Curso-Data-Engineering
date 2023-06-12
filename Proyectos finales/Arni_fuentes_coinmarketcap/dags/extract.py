import requests
from key import API_KEY
import pandas as pd


def extract_data():
    URL = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': API_KEY,
    }

    response = requests.get(url=URL, headers=headers)
    data = response.json()

    dictionaryList = data['data']

    listWithAllRecords = []

    for dictionary in dictionaryList:
        id = dictionary['id']
        name = dictionary['name']
        symbol = dictionary['symbol']
        slug = dictionary['slug']
        num_market_pairs = dictionary['num_market_pairs']
        circulating_supply = dictionary['circulating_supply']
        cmc_rank = dictionary['cmc_rank']
        price = dictionary['quote']['USD']['price']
        last_updated = dictionary['last_updated']
        timestamp = dictionary['last_updated'][0:10]
        record = [
            id,
            name,
            symbol,
            slug,
            num_market_pairs,
            circulating_supply,
            cmc_rank,
            price,
            last_updated,
            timestamp
        ]

        listWithAllRecords.append(record)

    # Nueva data obtenida de la API
    df = pd.DataFrame(
        data=listWithAllRecords,
        columns=[
            'id',
            'name',
            'symbol',
            'slug',
            'num_market_pairs',
            'circulating_supply',
            'cmc_rank',
            'price',
            'last_updated',
            'timestamp'
        ]
    )
    print('dataframe generado.')
    return df
