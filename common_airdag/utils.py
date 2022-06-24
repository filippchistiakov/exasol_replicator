import os
import pandas as pd
from pyhocon import ConfigFactory


def load_config(file_dir, config_file='config.conf'):
    config = ConfigFactory.parse_file(os.path.join(file_dir, f'{config_file}'))
    if 'airflow' in config:
        if 'start_date' in config['airflow']:
            config['airflow']['start_date'] = pd.to_datetime(
                config['airflow']['start_date']).to_pydatetime()
    return config


def load_sql_file(file_dir, file_name, path='sql', replace=True, where_condition="", **replace_dict):
    with open(os.path.join(file_dir, path, file_name)) as f:
        sql_text = f.read()
    if replace:
        for k, v in replace_dict.items():
            sql_text = sql_text.replace(f"${k}", v)
    return sql_text

def failure_callback(context):
    pass