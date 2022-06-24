MERGE INTO "$target_schema"."$target_table_name" AS T
      USING (
      SELECT *
      FROM(
      IMPORT FROM jdbc at $source_jdbc_connection
      statement 'select $str_mysql_columns_jdbc from $source_schema.$source_table_name WHERE $where_list'
      )
      ) AS S
      ON $join_cols
      WHEN MATCHED THEN UPDATE SET $str_exa_columns_when_matched_update
      WHEN NOT MATCHED THEN INSERT VALUES ($str_mysql_columns)


