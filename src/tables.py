#!/usr/bin/env python3
"""tables.py

This module is used to initialize tables in a BigQuery dataset.

Example:
    $ python tables.py
"""

import os
from google.cloud import bigquery

import utils.config as config

BQ_CONFIG = config.get_config('bigquery')
CREDENTIALS_CONFIG = config.get_config('credentials')

SERVICE_ACCOUNT_PATH = os.path.join('./credentials', CREDENTIALS_CONFIG['bigquery'])
CLIENT = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)

def main():
    """Main

    Creates tables in BigQuery based on schema configurations in `config.yaml`.
    """

    dataset_ref = CLIENT.dataset(BQ_CONFIG['dataset_id'])

    tables = list(CLIENT.list_tables(dataset_ref))
    table_ids = [item.table_id for item in tables]

    for schema in BQ_CONFIG['schemas']:
        if not (schema['name'] in table_ids):
            _table_ref = dataset_ref.table(schema['name'])
            _schema = []

            for field in schema['fields']:
                _field = bigquery.SchemaField(name=field['name'], field_type=field['type'], mode=field['mode'])
                _schema.append(_field)

            _table = bigquery.Table(_table_ref, schema=_schema)
            _table = CLIENT.create_table(_table)

if __name__ == "__main__":
    main()
