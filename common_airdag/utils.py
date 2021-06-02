import ast
import hashlib
import os
import re
from urllib.parse import urlsplit, parse_qs
import pgpy

import pandas as pd
from airflow.models import Variable
# from airflow.operators.slack_operator import SlackAPIPostOperator
from common_airdag.slack import send_message_to_slack_channel
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


def parse_url_to_dict(url):
    url_split = urlsplit(url)
    return {
        "scheme": url_split.scheme,
        "netloc": url_split.netloc,
        "path": url_split.path,
        "fragment": url_split.fragment,
        "params": parse_qs(url_split.query),
    }


def md5(sting_to_md5):
    result = hashlib.md5(sting_to_md5.encode())
    return result.hexdigest()


def chunks(n, iterable):
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]

def failure_callback(context):
    """
    The function that will be executed on failure.

    :param context: The context of the executed task.
    :type context: dict
    """
    message = (
        'AIRFLOW TASK FAILURE TIPS:\n'
        'DAG:    {}\n'
        'TASKS:  {}\n'
        'Reason: {}\n'.format(
            context['task_instance'].dag_id, context['task_instance'].task_id, context['exception'],
        )
    )
    send_message_to_slack_channel(message, '#dwh_alerts')