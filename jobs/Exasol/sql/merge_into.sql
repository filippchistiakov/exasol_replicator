MERGE INTO "$target_schema"."$target_table_name" AS T
      USING (
      SELECT *
      FROM(
      IMPORT FROM jdbc at dwhreader
      statement 'select $str_mysql_columns_jdbc from $source_schema.$source_table_name WHERE $where_list'
      )
      ) AS S
      ON T.$target_main_field = S."$source_main_field"
      WHEN MATCHED THEN UPDATE SET $str_exa_columns
      WHEN NOT MATCHED THEN INSERT VALUES ($str_mysql_columns)


