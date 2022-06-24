import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from common_airdag.exasol_replicator_operators import do_magic_with_table
from common_airdag.utils import load_config, failure_callback

file_dir = os.path.dirname(__file__)
config = load_config(file_dir=file_dir
                     , config_file="dag_exasol_replicator.conf")
tables_config = load_config(file_dir=file_dir
                            , config_file="main_tables.conf")
tables = tables_config["tables"]

dag = DAG(
    "exa_REPLICA",
    catchup=False,
    max_active_runs=config["airflow"]['max_active_runs'],
    start_date=config["airflow"]['start_date'],
    default_args=config["airflow"],
    schedule_interval=config["schedule_interval_prediction"],
    concurrency=config["airflow"]['concurrency'],
    is_paused_upon_creation=True,
    on_failure_callback=failure_callback,
    tags=['EXASOL', 'REPLICA', 'MAIN']
)
with dag:
    tasks = []

    end_process = DummyOperator(task_id='end_exa_replica')

    for table in tables:
        replicator_task = PythonOperator(
            task_id=f"{table['target_table_name']}",
            python_callable=do_magic_with_table,
            op_kwargs={
                'table': table
            },
            trigger_rule=TriggerRule.ALL_DONE,
            on_failure_callback=failure_callback
            # execution_timeout=dt.timedelta(minutes=30),
        )
        tasks.append(replicator_task)
        replicator_task >> end_process

if __name__ == '__main__':
    pass
