# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import pymysql
import numpy as np
from datetime import datetime

from airflow.hooks.dbapi_hook import DbApiHook
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from contextlib import closing
#from shapely import geometry


class MySqlHook(DbApiHook):
    """
    Interact with MySQL.
    You can specify charset in the extra field of your connection
    as ``{"charset": "utf8"}``. Also you can choose cursor as
    ``{"cursor": "SSCursor"}``. Refer to the MySQLdb.cursors for more details.
    """

    conn_name_attr = 'mysql_conn_id'
    default_conn_name = 'mysql_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(MySqlHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def get_conn(self):
        """
        Returns a mysql connection object
        """
        conn = self.get_connection(self.mysql_conn_id)
        conn_config = {
            "user": conn.login,
            "passwd": conn.password or '',
            "host": conn.host or 'localhost',
            "db": self.schema or conn.schema or '',
            'charset': 'utf8',
            'use_unicode': True
        }

        if not conn.port:
            conn_config["port"] = 3306
        else:
            conn_config["port"] = int(conn.port)

        conn = pymysql.connect(**conn_config)
        return conn

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000):
        """
        A generic way to insert a set of tuples into a table,
        a new transaction is created every commit_every rows

        :param table: Name of the target table
        :type table: str
        :param rows: The rows to insert into the table
        :type rows: iterable of tuples
        :param target_fields: The names of the columns to fill in the table
        :type target_fields: iterable of strings
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :type commit_every: int
        """
        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = "({})".format(target_fields)
        else:
            target_fields = ''

        if len(rows) == 0:
            logging.warning(f'No rows to insert in table {table}')

        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, False)

            conn.commit()

            with closing(conn.cursor()) as cur:
                for i, row in enumerate(rows, 1):
                    l = []
                    for cell in row:
                        l.append(self._serialize_cell(cell, conn))
                    sql = "INSERT INTO {0} {1} VALUES ({2});".format(
                        table,
                        target_fields,
                        ",".join(l))
                    cur.execute(sql)
                    if commit_every and i % commit_every == 0:
                        conn.commit()
                        logging.info(
                            f"Loaded {i} into {table} rows so far")

            conn.commit()
        logging.info(f"Done loading. Loaded a total of {len(rows)} rows")

    def insert_df(self, table, df):
        rows = df.values.tolist()
        return self.insert_rows(table, rows,
                                target_fields=df.columns.tolist())

    def bulk_load(self, table, tmp_file):
        """
        Loads a tab-delimited file into a database table
        """
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("""
            LOAD DATA LOCAL INFILE '{tmp_file}'
            INTO TABLE {table}
            """.format(**locals()))
        conn.commit()

    @staticmethod
    def _serialize_cell(cell, conn=None):
        """
        MySQLdb converts an argument to a literal when passing those seperately to execute.
        Hence, this method does nothing.
        :param cell: The cell to insert into the table
        :type cell: object
        :param conn: The database connection
        :type conn: connection object
        :return: The same cell
        :rtype: object
        """
        if isinstance(cell, datetime):
            return "'{}'".format(cell.isoformat())
        #if isinstance(cell, geometry.polygon.Polygon):
        #    return "ST_GEOMFROMTEXT('{}')".format(cell)
        if isinstance(cell, str):
            return "'{}'".format(cell)
        if cell is None:
            return 'NULL'
        if np.isnan(cell):
            return 'NULL'
        return str(cell)


class MySqlOperator(BaseOperator):
    """
    Executes sql code in a specific MySQL database

    :param mysql_conn_id: reference to a specific mysql database
    :type mysql_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param database: name of database which overwrite defined one in connection
    :type database: string
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql, mysql_conn_id='mysql_default', parameters=None,
            autocommit=False, database=None, *args, **kwargs):
        super(MySqlOperator, self).__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id,
                         schema=self.database)
        hook.run(
            self.sql,
            autocommit=self.autocommit,
            parameters=self.parameters)


class MySqlSensor(BaseSensorOperator):
    """
    Runs a sql statement until a criteria is met. It will keep trying while
    sql returns no row, or if the first cell in (0, '0', '').

    :param conn_id: The connection to run the sensor against
    :type conn_id: string
    :param sql: The sql to run. To pass, it needs to return at least one cell
        that contains a non-zero / empty string value.
    """
    template_fields = ('sql',)
    template_ext = ('.hql', '.sql',)
    ui_color = '#7c7287'

    @apply_defaults
    def __init__(self, conn_id, sql, poke_interval=3, *args, **kwargs):
        self.sql = sql
        self.conn_id = conn_id
        super(MySqlSensor, self).__init__(*args, **kwargs)
        self.poke_interval = poke_interval

    def poke(self, context):
        hook = MySqlHook(self.conn_id)
        logging.info('Poking: %s', self.sql)
        records = hook.get_pandas_df(self.sql)
        if len(records) == 0:
            return False
        else:
            if str(records.values[0][0]) in ('0', '',):
                return False
            else:
                return True
