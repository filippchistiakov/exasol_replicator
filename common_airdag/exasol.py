# -*- coding: utf-8 -*-
# Это модифицированный airflow.providers.exasol.hooks.exasol

from airflow.hooks.dbapi_hook import DbApiHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyexasol.db2 import connect


class ExasolHook(DbApiHook):
    """
    Interact with Exasol.
    """

    conn_name_attr = 'exasol_conn_id'
    default_conn_name = 'exasol_default'
    supports_autocommit = True

    def get_conn(self):
        """
        Returns pyexasol connection object
        """
        conn = self.get_connection(self.exasol_conn_id)

        if not conn.port:
            port = '8563'
        else:
            port = str(conn.port)

        conn_config = {
            "user": conn.login,
            "password": conn.password or '',
            "schema": conn.schema or '',
            "dsn": (conn.host or 'localhost') + ':' + port
        }
        conn = connect(**conn_config)
        return conn

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000, replace=False):
        with self.get_conn() as conn:
            conn.import_from_iterable(rows, table, import_params={'columns': target_fields})
            conn.commit()
        self.log.info("Done loading. Loaded a total of %s rows", len(rows))

    def insert_df(self, table, df):
        with self.get_conn() as conn:
            conn.import_from_pandas(df, table)
            conn.commit()
            self.log.info("Done loading. Loaded a total of %s rows", df.shape[0])


class ExasolOperator(BaseOperator):
    """
    Executes sql code in a specific Exasol database

    :param sql_file_nm: the sql code to be executed. (templated)
    :type sql_file_nm: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param exasol_conn_id: reference to a specific postgres database
    :type exasol_conn_id: str
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: mapping or iterable
    :param database: name of database which overwrite defined one in connection
    :type database: str
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql,
            exasol_conn_id='exasol',
            autocommit=False,
            parameters=None,
            *args, **kwargs):
        super(ExasolOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.exasol_conn_id = exasol_conn_id
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = ExasolHook(exasol_conn_id=self.exasol_conn_id)
        hook.run(self.sql, self.autocommit, parameters=self.parameters)