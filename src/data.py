#!/usr/bin/env python3
"""data.py

This module is used to bootstrap tables in a BigQuery database.

Example:
    $ python data.py
"""

import os
import logging
import sys
import pandas as pd

from pandas_gbq import to_gbq
from google.cloud import bigquery

import utils.config as config

DATA_CONFIG = config.get_config('data')
BQ_CONFIG = config.get_config('bigquery')
CREDENTIALS_CONFIG = config.get_config('credentials')

DATA_BASE_URL = DATA_CONFIG['base_url']
DATA_BASE_DIR = DATA_CONFIG['base_dir']
SERVICE_ACCOUNT_PATH = os.path.join('./credentials', CREDENTIALS_CONFIG['bigquery'])
CLIENT = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)

logger = logging.getLogger('pandas_gbq')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))


def _get_data(csv_path, location):
    assert location in ['url', 'dir'], '`location` must be one of "url" or "dir".'
    output = pd.DataFrame()

    if not isinstance(csv_path, list):
        csv_path = [csv_path]

    for path in csv_path:
        data_loc = ''
        if location == 'url':
            data_loc = DATA_BASE_URL + path
        elif location == 'dir':
            data_loc = os.path.join(DATA_BASE_DIR, path)

        print('Loading data from {}'.format(data_loc))
        output = output.append(pd.read_csv(data_loc, low_memory=False))

    return output


def _rename(data, table_id):
    output = data.copy()
    
    schemas = BQ_CONFIG['schemas']
    table_schema = [ schema for schema in schemas if schema['name'] == table_id ][0]
    rename_map = { f['original_name']: f['name'] for f in table_schema['fields'] }

    output = output.rename(columns=rename_map)

    return output


def _to_bq(data, table_id):
    project_id = BQ_CONFIG['project_id']
    dataset_id = BQ_CONFIG['dataset_id']

    dataset_ref = CLIENT.dataset(dataset_id)

    tables = list(CLIENT.list_tables(dataset_ref))
    table_ids = [ item.table_id for item in tables ]

    assert table_id in table_ids, 'table_id must already exist to add data'

    table_name = dataset_id + '.' + table_id

    print('Writing to {}.'.format(table_name))
    data.to_gbq(project_id=project_id, destination_table=table_name, if_exists='replace',
                private_key=SERVICE_ACCOUNT_PATH)


def main():
    """Main"""

    print('LOADING DATA')
    df_games = _get_data(DATA_CONFIG['games_url'], 'url')
    df_players = _get_data(DATA_CONFIG['players_url'], 'url')
    df_plays = _get_data(DATA_CONFIG['plays_url'], 'url')
    df_tracking = _get_data(DATA_CONFIG['tracking_url'], 'url')
    df_nflscrapr = _get_data(DATA_CONFIG['nflscrapr_path'], 'dir')

    print('RENAMING DATA')
    df_games = _rename(df_games, 'games')
    df_players = _rename(df_players, 'players')
    df_plays = _rename(df_plays, 'plays')
    df_tracking = _rename(df_tracking, 'tracking')
    df_nflscrapr = _rename(df_nflscrapr, 'nflscrapr')

    print('WRITING DATA TO BIGQUERY')
    _to_bq(df_games, 'games')
    _to_bq(df_players, 'players')
    _to_bq(df_plays, 'plays')
    _to_bq(df_tracking, 'tracking')
    _to_bq(df_nflscrapr, 'nflscrapr')


if __name__ == "__main__":
    main()
