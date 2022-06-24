import datetime as dt
import logging
import os

import pandas as pd
from common_airdag.exasol import ExasolHook
from common_airdag.mysql import MySqlHook
from common_airdag.utils import load_sql_file

file_dir = os.path.dirname(__file__)


def recreate_script():
    if True:
        return None
    exa = ExasolHook("exasol_local")
    logging.warning("Script migration replaced!")
    create_or_replace_script_migration_sql = load_sql_file(
        file_dir,
        path=os.path.join("sql", "exasol_replicator", "mysql"),
        file_name="create_or_replace_script_migration_MYSQL_TO_EXASOL.sql",
    )

    logging.info(f"start - {create_or_replace_script_migration_sql}")
    exa.run(create_or_replace_script_migration_sql)
    return


def validate_table_params(table):
    source_schema = table.get("source_schema")
    source_table_name = table.get("source_table_name")
    source_database_type = table.get("source_database_type")
    if source_database_type not in ["mysql", "postgresql"]:
        raise ValueError(f"incorect source_database_type {source_database_type}")
    source_jdbc_connection = table.get("source_jdbc_connection")
    if source_jdbc_connection not in ["DWHREADER", "FATPAY"]:
        raise ValueError(f"incorect source_jdbc_connection {source_jdbc_connection}")
    target_schema = table.get("target_schema")
    target_table_name = table.get("target_table_name")

    load_type = table.get("load_type") if "load_type" in table else False
    source_merge_columns = (
        table.get("source_merge_columns") if "source_merge_columns" in table else False
    )
    source_main_fields = (
        table.get("source_main_fields") if "source_main_fields" in table else False
    )
    target_main_fields = (
        table.get("target_main_fields") if "target_main_fields" in table else False
    )
    source_freq = table.get("source_freq") if "source_freq" in table else {}
    data_filed = source_freq.get('data_filed') if "data_filed" in source_freq else False
    freq = source_freq.get('freq') if "data_filed" in source_freq else False
    start_year = source_freq.get('start_year') if "data_filed" in source_freq else False
    replace_fields_to = table.get('replace_fields_to') if "replace_fields_to" in table else False

    logging.info(f"replace_fields_to {replace_fields_to}")

    if not isinstance(target_main_fields, list):
        raise ValueError(f'target_main_fields "{target_main_fields}" is not list')
    if not isinstance(source_main_fields, list):
        raise ValueError(f'target_main_fields "{target_main_fields}" is not list')

    # target_main_field = target_main_fields[0]
    # source_main_field = source_main_fields[0]

    # Проверка корректности конфиг файла
    if load_type not in ["merge", "truncate"]:
        raise ValueError(f'load_type "{load_type}" not in list [merge, truncate]')
    elif load_type == "merge":
        # Была проверка по типам, убрал изза смены на list
        # if not (isinstance(source_main_field, str) & isinstance(target_main_field, str)):
        #    raise ValueError(f"bad main_fileds {source_main_field}, {target_main_field}")
        for field, field_type in source_merge_columns.items():
            if field_type not in ["int", "datetime"]:
                raise ValueError(f"incorect field_type {field_type} for FIELD {field}")
            # elif (field != source_main_field) & (field_type == "int"):
            #    raise ValueError(
            #        f"field_type {field_type} for FIELD {field}, wrong. Only source_main_field can be INT")
    result = {
        "load_type": load_type,
        "source_schema": source_schema,
        "source_table_name": source_table_name,
        "source_main_fields": source_main_fields,
        # 'source_main_field': source_main_field,
        "source_merge_columns": source_merge_columns,
        "source_jdbc_connection": source_jdbc_connection,
        "source_database_type": source_database_type,
        "target_schema": target_schema,
        "target_table_name": target_table_name,
        "target_main_fields": target_main_fields,
        "data_filed": data_filed,
        "freq": freq,
        "start_year": start_year,
        # 'target_main_field': target_main_field
        "replace_fields_to": replace_fields_to
    }
    return result


def create_table_if_not_exist(
        exa,
        source_schema,
        source_table_name,
        target_schema,
        target_table_name,
        source_jdbc_connection,
):
    sql_from_file = load_sql_file(file_dir=file_dir, file_name="check_exa_have_table.sql",
                                  path=os.path.join("sql", "exasol_replicator", "exasol"), target_schema=target_schema,
                                  target_table_name=target_table_name, )

    logging.info(f"start - {sql_from_file}")
    exasol_have_table = (
            exa.get_first(
                sql_from_file
            )
            is not None
    )

    if not exasol_have_table:
        # Если целевой таблицы в Exasol нет, создаем схему и таблицу
        logging.info(f"Create table Exasol {target_schema}.{target_table_name}")
        exa_resp = run_script_migration_helper(
            exa,
            source_schema,
            source_table_name,
            target_schema,
            target_table_name,
            source_jdbc_connection,
        )

        logging.info(exa_resp["SQL_CREATE_SCHEMA"])
        exa.run(exa_resp["SQL_CREATE_SCHEMA"])

        logging.info(exa_resp["SQL_CREATE_TABLE"])
        exa.run(exa_resp["SQL_CREATE_TABLE"])


def run_script_migration_helper(
        exa,
        source_schema,
        source_table_name,
        target_schema,
        target_table_name,
        source_jdbc_connection,
):
    if source_jdbc_connection == "DWHREADER":
        run_script_migration_sql = load_sql_file(
            file_dir=file_dir,
            file_name="run_script_migration.sql",
            path=os.path.join("sql", "exasol_replicator", "mysql"),
            source_schema=source_schema,
            source_table_name=source_table_name,
            target_schema=target_schema,
            target_table_name=target_table_name,
        )

        logging.info(f"start - {run_script_migration_sql}")
        return exa.get_pandas_df(run_script_migration_sql).T.to_dict()[0]


def create_where_list(source_merge_columns, execution_date, max_target_main_field):
    where_list_def = ""
    last_i = len(source_merge_columns.keys()) - 1
    for i, (FIELD, FIELD_TYPE) in enumerate(source_merge_columns.items()):
        start_date_for_where = f'"{execution_date}"'
        where_list_def += f"{FIELD} >= {start_date_for_where if FIELD_TYPE == 'datetime' else max_target_main_field} "
        if i != last_i:
            where_list_def += "OR "
    return where_list_def


def create_join_cols(target_main_fields, source_main_fields):
    join_cols = ""
    for a, b in zip(target_main_fields, source_main_fields):
        join_cols += f'T.{a} = S."{b}" AND '
    join_cols = join_cols[:-4]
    return join_cols


def get_dict_columns(
        exa,
        source_db,
        source_schema,
        source_table_name,
        target_schema,
        target_table_name,
        target_main_fields,
        *args
):
    replace_fields_to = args[0]

    logging.info(f"args {args}")
    logging.info(f"replace fields_to in get_dict_columns {replace_fields_to}")

    mysql_columns_sql = f"""select column_name AS COLUMN_NAME
                           from information_schema.columns
                           join information_schema.tables using (table_catalog, table_schema, table_name)
                           where table_schema = "{source_schema}"
                           AND table_name = "{source_table_name}" """
    exasol_columns_sql = f"""SELECT COLUMN_NAME
                            FROM SYS.EXA_ALL_COLUMNS
                            WHERE COLUMN_SCHEMA = '{target_schema}'
                            AND COLUMN_TABLE = '{target_table_name}' """

    logging.info(f"mysql_columns_sql [{mysql_columns_sql}]")
    logging.info(f"exasol_columns_sql [{exasol_columns_sql}]")
    dict_columns = dict(
        zip(
            list(source_db.get_pandas_df(mysql_columns_sql)["COLUMN_NAME"].values),
            list(exa.get_pandas_df(exasol_columns_sql)["COLUMN_NAME"].values),
        )
    )
    str_exa_columns_when_matched_update = "".join(
        f'T.{t} = S."{s}",' if t not in target_main_fields else ""
        for s, t in dict_columns.items()
    )[:-1]

    if replace_fields_to:
        str_mysql_columns_jdbc = ", ".join(
            f"`{x}`" if x not in replace_fields_to
            else replace_fields_to[x]
            for x in dict_columns.keys()
        )

        str_mysql_columns = ", ".join(
            f'"{x}"' if x not in replace_fields_to
            else replace_fields_to[x]
            for x in dict_columns.keys()
        )

    else:
        str_mysql_columns_jdbc = ", ".join(f"`{x}`" for x in dict_columns.keys())
        str_mysql_columns = ", ".join(f'"{x}"' for x in dict_columns.keys())

    logging.info(f"columns have been builded_jdbc {str_mysql_columns_jdbc}")
    logging.info(f"columns have been builded {str_mysql_columns}")

    return (
        str_exa_columns_when_matched_update,
        str_mysql_columns_jdbc,
        str_mysql_columns,
    )


def try_full_insert_with_split_by_dt(
        exa,
        source_merge_columns,
        source_schema,
        source_table_name,
        target_schema,
        target_table_name,
        source_jdbc_connection,
        data_filed,
        freq,
        start_year
):
    logging.info(f"try_full_insert_with_split_by_dt start")
    exa_resp = run_script_migration_helper(
        exa,
        source_schema,
        source_table_name,
        target_schema,
        target_table_name,
        source_jdbc_connection,
    )
    # Если есть словарь со списком колонок
    logging.info(f"source_merge_columns {source_merge_columns}")
    logging.info(f"exa_resp {exa_resp}")
    logging.info(f"{data_filed} and {freq} and {start_year}")
    if data_filed and freq and start_year:
        # https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases
        dt_list = pd.date_range(
            start=dt.datetime(start_year, 1, 1),
            end=dt.datetime.now(),
            freq='freq',
        ).tolist()
        for x, y in list(zip(dt_list, dt_list[1:] + dt_list[:-1])):
            if x < y:
                where_list = (
                    f' WHERE {data_filed} >= "{x}" AND {data_filed} < "{y}" \''
                )
            else:
                where_list = f' WHERE {data_filed} >= "{x}" \''
            logging.info(f"where_list = {where_list}")
            current_import = exa_resp["SQL_IMPORTS"][:-1] + where_list
            logging.info(f"query - {current_import}")
            exa.run(current_import)
    else:
        exa_resp_sql_imports = exa_resp["SQL_IMPORTS"]
        logging.info(f"query will be start [{exa_resp_sql_imports}]")
        exa.run(exa_resp_sql_imports)


def do_magic_with_table(table, **kwargs):
    # Загружаем параметры репликации из конфига
    table_params = validate_table_params(table)
    load_type = table_params.get("load_type")
    source_jdbc_connection = table_params.get("source_jdbc_connection")
    source_database_type = table_params.get("source_database_type")
    source_schema = table_params.get("source_schema")
    source_table_name = table_params.get("source_table_name")
    source_main_fields = table_params.get("source_main_fields")
    # source_main_field = table_params.get('source_main_field')
    source_merge_columns = table_params.get("source_merge_columns")
    target_schema = table_params.get("target_schema")
    target_table_name = table_params.get("target_table_name")
    target_main_fields = table_params.get("target_main_fields")
    # target_main_field = table_params.get('target_main_field')
    data_filed = table_params.get('data_filed')
    freq = table_params.get('freq')
    start_year = table_params.get('start_year')
    replace_fields_to = table_params.get("replace_fields_to")
    # Основные переменные
    exa = ExasolHook("exasol_local")
    if source_database_type == "mysql":
        source_db = MySqlHook("mysql")
    else:
        source_db = MySqlHook("mysql")

    execution_date = str(kwargs["execution_date"])[0:10]
    execution_date = dt.datetime.strptime(execution_date, "%Y-%m-%d") - dt.timedelta(
        days=3
    )

    # Начало репликации
    # Проверяем на наличие целевой таблицы и создаем если ее нет в Exasol
    create_table_if_not_exist(
        exa,
        source_schema,
        source_table_name,
        target_schema,
        target_table_name,
        source_jdbc_connection,
    )

    logging.info(f"table [{target_table_name}] in schema [{target_schema}]")

    # Начинаем репликацию исходя из типа загрузки
    if load_type == "merge":
        # если строк в таблице записанных нет, то делаем полный инсерт
        name_ = exa.get_first(load_sql_file(file_dir=file_dir, file_name="check_exa_have_rows.sql",
                                            path=os.path.join("sql", "exasol_replicator", "exasol"),
                                            target_schema=target_schema, target_table_name=target_table_name, ))[0]
        exasol_have_rows = (
                name_
                > 0
        )
        if exasol_have_rows:
            logging.info("rows already in target table, use merge request")
            logging.info("start create merge query")
            #

            sql = load_sql_file(
                file_dir=file_dir,
                file_name="get_max_val.sql",
                path=os.path.join("sql", "exasol_replicator", "exasol"),
                target_schema=target_schema,
                target_table_name=target_table_name,
                target_main_field=target_main_fields[0],
            )
            logging.info(f"start {sql}")
            max_target_main_field = exa.get_first(
                sql
            )[0]

            logging.info(f"max target main field {max_target_main_field}")

            where_list = create_where_list(
                source_merge_columns, execution_date, max_target_main_field
            )

            logging.info(f"where list have been created [{where_list}]")

            join_cols = create_join_cols(target_main_fields, source_main_fields)

            logging.info(f"join cols [{join_cols}]")

            (
                str_exa_columns_when_matched_update,
                str_mysql_columns_jdbc,
                str_mysql_columns,
            ) = get_dict_columns(
                exa,
                source_db,
                source_schema,
                source_table_name,
                target_schema,
                target_table_name,
                target_main_fields,
                replace_fields_to
            )

            logging.info(
                f"""
                    str_exa_columns_when_matched_update - {str_exa_columns_when_matched_update},
                    str_mysql_columns_jdbc -{str_mysql_columns_jdbc},
                    str_mysql_columns - {str_mysql_columns}
                """
            )

            merge_into_sql = load_sql_file(
                file_dir=file_dir,
                file_name="merge_into.sql",
                path=os.path.join("sql", "exasol_replicator", "exasol"),
                target_schema=target_schema,
                target_table_name=target_table_name,
                source_schema=source_schema,
                source_table_name=source_table_name,
                str_mysql_columns_jdbc=str_mysql_columns_jdbc,
                str_mysql_columns=str_mysql_columns,
                str_exa_columns_when_matched_update=str_exa_columns_when_matched_update,
                where_list=where_list,
                join_cols=join_cols,
                source_jdbc_connection=source_jdbc_connection,
            )

            logging.info(f"query will be start [{merge_into_sql}]")
            exa.run(merge_into_sql)
        else:
            # Если ничего нет еще в таблице то импортируем все
            # Если таблица есть но ничего в ней нет, будет ошибка, так как exa_resp не сущесвует
            try_full_insert_with_split_by_dt(
                exa,
                source_merge_columns,
                source_schema,
                source_table_name,
                target_schema,
                target_table_name,
                source_jdbc_connection,
                data_filed,
                freq,
                start_year
            )

    elif load_type == "truncate":
        logging.info(f"start query TRUNCATE TABLE {target_schema}.{target_table_name}")
        exa.run(f"TRUNCATE TABLE {target_schema}.{target_table_name}")
        try_full_insert_with_split_by_dt(
            exa,
            source_merge_columns,
            source_schema,
            source_table_name,
            target_schema,
            target_table_name,
            source_jdbc_connection,
            data_filed,
            freq,
            start_year
        )
    check_replication(table)
    return


def drop_table(table, **kwargs):
    table_params = validate_table_params(table)
    target_schema = table_params.get("target_schema")
    target_table_name = table_params.get("target_table_name")
    exa = ExasolHook("exasol_local")

    logging.info(f'start query - DROP TABLE IF EXISTS {target_schema}.{target_table_name}')
    exa.run(f'DROP TABLE IF EXISTS {target_schema}.{target_table_name}')


def check_replication(table):
    source_schema = table.get("source_schema")
    source_table_name = table.get("source_table_name")
    source_merge_columns = (
        table.get("source_merge_columns") if "source_merge_columns" in table else False
    )

    target_schema = table.get("target_schema")
    target_table_name = table.get("target_table_name")
    source_database_type = table.get("source_database_type")

    exa = ExasolHook("exasol_local")
    if source_database_type == "mysql":
        source_db = MySqlHook("mysql")
    else:
        source_db = MySqlHook("mysql")
    if source_merge_columns:
        val: str
        list_dt_cols = [
            val
            for val in source_merge_columns
            if source_merge_columns[val] == "datetime"
        ]
        if "created_at" in list_dt_cols:
            check_dttm = str(
                (dt.datetime.now() - dt.timedelta(minutes=30)).replace(
                    minute=0, second=0, microsecond=0
                )
            )

            mysql_count_sql = f"SELECT count(1) FROM {source_schema}.{source_table_name} WHERE created_at <= '{check_dttm}'"
            logging.info(f"start [{mysql_count_sql}]")
            mysql_count = source_db.get_first(
                mysql_count_sql
            )[0]

            exa_count_sql = f"SELECT count(1) FROM {target_schema}.{target_table_name} WHERE CREATED_AT <= '{check_dttm}'"
            logging.info(f"start [{exa_count_sql}]")
            exa_count = exa.get_first(
                exa_count_sql
            )[0]
            logging.info(f"created_at {mysql_count} {exa_count}")
            logging.info(mysql_count >= exa_count)
            assert exa_count >= mysql_count
    else:
        mysql_count_sql = f"SELECT count(1) FROM {source_schema}.{source_table_name}"

        logging.info(f"start [{mysql_count_sql}]")
        mysql_count = source_db.get_first(
            mysql_count_sql
        )[0]

        exa_count_sql = f"SELECT count(1) FROM {target_schema}.{target_table_name}"

        logging.info(f"start [{exa_count_sql}]")
        exa_count = exa.get_first(
            exa_count_sql
        )[0]
        logging.info(f"f + 1000 {mysql_count} {exa_count}")
        logging.info(exa_count + 1000 >= mysql_count)
        assert exa_count + 1000 >= mysql_count
    return
