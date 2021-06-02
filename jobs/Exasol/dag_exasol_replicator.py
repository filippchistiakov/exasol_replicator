import datetime as dt
import logging
import os
import socket
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from common_airdag.exasol import ExasolHook
from common_airdag.mysql import MySqlHook
from common_airdag.utils import load_config, failure_callback, load_sql_file

file_dir = os.path.dirname(__file__)
config = load_config(file_dir=file_dir
                     , config_file="dag_exasol_replicator.conf")
tables = config["tables_list"]
EXA_NAME = 'exasol'
MYSQL_NAME = 'mysql'

# def recreate_script(**kwargs):
#    exa = ExasolHook('exasol_local')
#    logging.warning('Script migration replaced!')
#    create_or_replace_script_migration_sql = load_sql_file(file_dir, 'create_or_replace_script_migration.sql')
#    exa.run(create_or_replace_script_migration_sql)
#    return

def base_def(**kwargs):
    return


def do_magic_with_table(table, **kwargs):
    exa = ExasolHook(EXA_NAME)
    mysql = MySqlHook(MYSQL_NAME)
    execution_date = str(kwargs["execution_date"])[0:10]
    execution_date = dt.datetime.strptime(execution_date, "%Y-%m-%d") - dt.timedelta(days=3)

    def run_script_migration_helper(source_schema,
                                    source_table_name,
                                    target_schema,
                                    target_table_name
                                    ):
        # Часть про создание таблицы
        run_script_migration_sql = load_sql_file(file_dir, "run_script_migration.sql",
                                                 source_schema=source_schema,
                                                 source_table_name=source_table_name,
                                                 target_schema=target_schema,
                                                 target_table_name=target_table_name
                                                 )
        return exa.get_pandas_df(run_script_migration_sql).T.to_dict()[0]

    def create_where_list(source_merge_columns, execution_date, max_target_main_field):
        where_list_def = ""
        last_i = len(source_merge_columns.keys()) - 1
        for i, (FIELD, FIELD_TYPE) in enumerate(source_merge_columns.items()):
            start_date_for_where = f'"{execution_date}"'
            where_list_def += f"{FIELD} >= {start_date_for_where if FIELD_TYPE == 'datetime' else max_target_main_field} "
            if i != last_i:
                where_list_def += 'OR '
        return where_list_def

    def get_dict_columns(source_schema, source_table_name, target_schema, target_table_name):
        logging.info(f" get_dict_columns {source_schema, source_table_name, target_schema, target_table_name}")
        return dict(
            zip(
                list(
                    mysql.get_pandas_df(
                        f"""select column_name
                                            from information_schema.columns
                                            join information_schema.tables using (table_catalog, table_schema, table_name)
                                            where table_schema = "{source_schema}"
                                            AND table_name = "{source_table_name}" """
                    )["column_name"].values
                ),
                list(
                    exa.get_pandas_df(
                        f"""SELECT COLUMN_NAME
                                FROM SYS.EXA_ALL_COLUMNS
                                WHERE COLUMN_SCHEMA = '{target_schema}'
                                AND COLUMN_TABLE = '{target_table_name}' """
                    )["COLUMN_NAME"].values
                ),
            )
        )

    def try_full_insert_with_split_by_dt(source_merge_columns, exa_resp):
        # Если есть словарь со списком колонок
        logging.info(f"source_merge_columns {source_merge_columns}")
        logging.info(f"exa_resp {exa_resp}")
        if source_merge_columns:
            list_dt_cols = [val for val in source_merge_columns if source_merge_columns[val] == 'datetime']
            # TODO: нужно добавить freq и start_year как параметр конфига таблицы
            if ("created_at" in list_dt_cols) | ("updated_at" in list_dt_cols) | ("EventTime" in list_dt_cols):
                if "created_at" in list_dt_cols:
                    data_filed = 'created_at'
                    freq = 'AS'
                    start_year = 2018
                elif "updated_at" in list_dt_cols:
                    data_filed = 'updated_at'
                    freq = 'AS'
                    start_year = 2018
                elif "EventTime" in list_dt_cols:
                    data_filed = 'EventTime'
                    freq = 'W-MON'
                    start_year = 2021
                dt_list = pd.date_range(start=dt.datetime(start_year, 1, 1), end=dt.datetime.now(), freq=freq).tolist()
                for x, y in list(zip(dt_list, dt_list[1:] + dt_list[:-1])):
                    if x < y:
                        where_list = f" WHERE {data_filed} >= \"{x}\" AND {data_filed} < \"{y}\" '"
                    else:
                        where_list = f" WHERE {data_filed} >= \"{x}\" '"
                    current_import = exa_resp["SQL_IMPORTS"][:-1] + where_list
                    exa.run(current_import)
        else:
            exa.run(exa_resp["SQL_IMPORTS"])

    # Загружаем параметры репликации из конфига
    source_schema = table.get("source_schema")
    source_table_name = table.get("source_table_name")

    target_schema = table.get("target_schema")
    target_table_name = table.get("target_table_name")

    load_type = table.get("load_type") if "load_type" in table else False
    source_merge_columns = (
        table.get("source_merge_columns") if "source_merge_columns" in table else False
    )
    source_main_field = (
        table.get("source_main_field") if "source_main_field" in table else False
    )
    target_main_field = (
        table.get("target_main_field") if "target_main_field" in table else False
    )
    # Проверка корректности конфиг файла
    if load_type not in ["merge", "truncate"]:
        raise ValueError(f'load_type "{load_type}" not in list [merge, truncate]')
    else:
        if load_type == 'merge':
            if not (isinstance(source_main_field, str) & isinstance(target_main_field, str)):
                raise ValueError(f"bad main_fileds {source_main_field}, {target_main_field}")
            for field, field_type in source_merge_columns.items():
                if field_type not in ["int", "datetime"]:
                    raise ValueError(f"incorect field_type {field_type} for FIELD {field}")
                elif (field != source_main_field) & (field_type == "int"):
                    raise ValueError(
                        f"field_type {field_type} for FIELD {field}, wrong. Only source_main_field can be INT")
    # Проверяем на наличие целевой таблицы в Exasol
    exasol_have_table = (
            exa.get_first(
                load_sql_file(
                    file_dir=file_dir,
                    file_name="check_exa_have_table.sql",
                    target_schema=target_schema,
                    target_table_name=target_table_name,
                )
            )
            is not None
    )

    if not exasol_have_table:
        # Если целевой таблицы в Exasol нет, создаем схему и таблицу
        logging.info(f"Create table Exasol {target_schema}.{target_table_name}")
        exa_resp = run_script_migration_helper(source_schema,
                                               source_table_name,
                                               target_schema,
                                               target_table_name
                                               )
        exa.run(exa_resp["SQL_CREATE_SCHEMA"])
        exa.run(exa_resp["SQL_CREATE_TABLE"])

    # Начинаем репликацию исходя из типа загрузки
    if table["load_type"] == "merge":
        # если строк в таблице записанных нет, то делаем полный инсерт
        exasol_have_rows = (
                exa.get_first(
                    load_sql_file(
                        file_dir=file_dir,
                        file_name="check_exa_have_rows.sql",
                        target_schema=target_schema,
                        target_table_name=target_table_name,
                    )
                )[0]
                > 0
        )
        if exasol_have_rows:
            logging.info('start create merge query')
            #
            dict_columns = get_dict_columns(source_schema, source_table_name, target_schema, target_table_name)
            max_target_main_field = exa.get_first(
                load_sql_file(
                    file_dir=file_dir,
                    file_name="get_max_val.sql",
                    target_schema=target_schema,
                    target_table_name=target_table_name,
                    target_main_field=target_main_field,
                )
            )[0]

            where_list = create_where_list(source_merge_columns, execution_date, max_target_main_field)
            str_exa_columns = "".join(
                f'T.{t} = S."{s}",' if t != target_main_field else "" for s, t in dict_columns.items())[:-1]
            merge_into_sql = load_sql_file(
                file_dir=file_dir,
                file_name="merge_into.sql",
                target_schema=target_schema,
                target_table_name=target_table_name,
                source_main_field=source_main_field,
                target_main_field=target_main_field,
                source_schema=source_schema,
                source_table_name=source_table_name,
                str_mysql_columns_jdbc=", ".join(f'`{x}`' for x in dict_columns.keys()),
                str_mysql_columns=", ".join(f'"{x}"' for x in dict_columns.keys()),
                str_exa_columns=str_exa_columns,
                where_list=where_list
            )
            exa.run(merge_into_sql)
        else:
            # Если ничего нет еще в таблице то импортируем все
            # Если таблица есть но ничего в ней нет, будет ошибка, так как exa_resp не сущесвует
            logging.info("Full insert")
            if 'exa_resp' not in locals():
                exa_resp = run_script_migration_helper(source_schema,
                                                       source_table_name,
                                                       target_schema,
                                                       target_table_name
                                                       )
            try_full_insert_with_split_by_dt(source_merge_columns, exa_resp)

    elif table["load_type"] == "truncate":
        exa.run(f'TRUNCATE TABLE {target_schema}.{target_table_name}')
        if 'exa_resp' not in locals():
            exa_resp = run_script_migration_helper(source_schema,
                                                   source_table_name,
                                                   target_schema,
                                                   target_table_name
                                                   )
        logging.info(f"try_full_insert_with_split_by_dt")
        try_full_insert_with_split_by_dt(source_merge_columns, exa_resp)
    return


def check_replication(table, **kwargs):
    source_schema = table.get("source_schema")
    source_table_name = table.get("source_table_name")
    source_merge_columns = (
        table.get("source_merge_columns") if "source_merge_columns" in table else False
    )

    target_schema = table.get("target_schema")
    target_table_name = table.get("target_table_name")

    exa = ExasolHook(EXA_NAME)
    mysql = MySqlHook(MYSQL_NAME)
    if source_merge_columns:
        list_dt_cols = [val for val in source_merge_columns if source_merge_columns[val] == 'datetime']
        if "created_at" in list_dt_cols:
            check_dttm = str((dt.datetime.now() - dt.timedelta(minutes=30)).replace(minute=0, second=0, microsecond=0))
            mysql_count = mysql.get_first(
                f"SELECT count(1) FROM {source_schema}.{source_table_name} WHERE created_at <= '{check_dttm}'")[
                0]
            exa_count = exa.get_first(
                f"SELECT count(1) FROM {target_schema}.{target_table_name} WHERE CREATED_AT <= '{check_dttm}'")[
                0]
            logging.info(f"created_at {mysql_count} {exa_count}")
            logging.info(mysql_count >= exa_count)
            assert exa_count >= mysql_count
    else:
        mysql_count = mysql.get_first(f"SELECT count(1) FROM {source_schema}.{source_table_name}")[0]
        exa_count = exa.get_first(f"SELECT count(1) FROM {target_schema}.{target_table_name}")[0]
        logging.info(f"f + 1000 {mysql_count} {exa_count}")
        logging.info(exa_count + 1000 >= mysql_count)
        assert exa_count + 1000 >= mysql_count
    return


dag = DAG(
    "exa_REPLICA",
    catchup=True,
    max_active_runs=config["airflow"]['max_active_runs'],
    start_date=config["airflow"]['start_date'],
    default_args=config["airflow"],
    schedule_interval=config["schedule_interval_prediction"],
    concurrency=config["airflow"]['concurrency'],
    is_paused_upon_creation=True,
    on_failure_callback=failure_callback
)
with dag:
    tasks = []
    base_task = PythonOperator(
        task_id=f"base_task",
        python_callable=base_def,
        trigger_rule=TriggerRule.ALL_DONE,
        on_failure_callback=failure_callback
        # execution_timeout=dt.timedelta(minutes=30),
    )
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
        base_task >> replicator_task

        check_replication_task = PythonOperator(
            task_id=f"{table['target_table_name']}_check",
            python_callable=check_replication,
            op_kwargs={
                'table': table
            },
            trigger_rule=TriggerRule.ALL_DONE,
            on_failure_callback=failure_callback
            # execution_timeout=dt.timedelta(minutes=30),
        )
        tasks.append(check_replication_task)
        replicator_task >> check_replication_task

if __name__ == '__main__':  # Если запустили файл руками, то
    logging.warning('Start - Running from the console')
    from airflow.models import TaskInstance

    execution_date = dt.datetime.now()
    for task in tasks:
        ti = TaskInstance(task=task, execution_date=execution_date)  # Создать инстанс задачи
        context = ti.get_template_context()
        context['debug'] = True  # Запуск в режиме debug
        context['execution_date'] = execution_date
        logging.warning(f'Running task {task}')
        task.execute(context)  # Запустить задачу
        logging.warning(f'Finish task {task}')
    logging.warning('Finish - Running from the console')
