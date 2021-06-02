create or replace script database_migration.MYSQL_TO_EXASOL(
    CONNECTION_NAME -- name of the database connection inside exasol -> e.g. mysql_db
    ,IDENTIFIER_CASE_INSENSITIVE -- true if identifiers should be stored case-insensitiv (will be stored upper_case)
    ,SOURCE_SCHEMA -- filter for the schemas to generate and load (except information_schema and pg_catalog) -> '%' to load all
    ,SOURCE_TABLE_NAME -- filter for the tables to generate and load -> '%' to load all
    ,TARGET_SCHEMA
    ,TARGET_TABLE_NAME
    ) RETURNS TABLE
    AS
    exa_upper_begin=''
    exa_upper_end=''

    if IDENTIFIER_CASE_INSENSITIVE == true then
        exa_upper_begin='upper('
        exa_upper_end=')'
    end

    suc, res = pquery([[

    with vv_mysql_columns as (
        select ]]..exa_upper_begin..[[table_catalog]]..exa_upper_end..[[ as "exa_table_catalog", ']]..TARGET_SCHEMA..[[' as "exa_table_schema", table_schema as "mysql_table_schema", table_name as "mysql_table_name", ']]..TARGET_TABLE_NAME..[[' as "exa_table_name", ]]..exa_upper_begin..[[column_name]]..exa_upper_end..[[ as "exa_column_name", mysql.* from
            (import from jdbc at ]]..CONNECTION_NAME..[[ statement
                'select table_catalog, table_schema, table_name, column_name, ordinal_position, data_type, column_type, character_maximum_length, numeric_precision, numeric_scale
                    from information_schema.columns join information_schema.tables using (table_catalog, table_schema, table_name)
                    where table_type = ''BASE TABLE''
                    AND table_schema not in (''information_schema'',''performance_schema'', ''mysql'')
                    AND table_schema = '']]..SOURCE_SCHEMA..[[''
                    AND table_name = '']]..SOURCE_TABLE_NAME..[[''
            ') as mysql
    )

    ,vv_create_schemas as(
        SELECT "mysql_table_schema" AS mysql_table_schema, "mysql_table_name" AS mysql_table_name,"exa_table_schema" AS exa_table_schema, "exa_table_name" AS exa_table_name, 'create schema if not exists "' || "exa_table_schema" || '";'  AS sql_create_schema from vv_mysql_columns  group by "exa_table_catalog","exa_table_schema", "exa_table_name", "mysql_table_schema", "mysql_table_name"  order by "exa_table_catalog","exa_table_schema"
    )

    ,vv_create_tables as (
        select "mysql_table_schema" AS mysql_table_schema, "mysql_table_name" AS mysql_table_name,"exa_table_schema" AS exa_table_schema, "exa_table_name" AS exa_table_name,'create table if not exists  "' || "exa_table_schema" || '"."' || "exa_table_name" || '" (' || group_concat(
        case
        -- ### numeric types ###
        when upper(data_type) = 'INT' then '"' || "exa_column_name" || '" ' || 'DECIMAL(11,0)'
        when upper(data_type) = 'INTEGER' then '"' || "exa_column_name" || '" ' || 'DECIMAL(11,0)'
        when upper(data_type) = 'TINYINT' then '"' || "exa_column_name" || '" ' || 'DECIMAL(4,0)'
        when upper(data_type) = 'SMALLINT' then '"' || "exa_column_name" || '" ' || 'DECIMAL(5,0)'
        when upper(data_type) = 'MEDIUMINT' then '"' || "exa_column_name" || '" ' || 'DECIMAL(9,0)'
        when upper(data_type) = 'BIGINT' then '"' || "exa_column_name" || '" ' || 'DECIMAL (20,0)'
        when upper(data_type) = 'FLOAT' then '"' || "exa_column_name" || '" ' || 'FLOAT'
        when upper(data_type) = 'DOUBLE' then '"' || "exa_column_name" || '" ' || 'DOUBLE'
        -- in mysql scale <= 30 and scale <= precision
        when upper(data_type) = 'DECIMAL' then case when numeric_precision is null then '"' || "exa_column_name" || '" ' || 'DOUBLE' else '"' || "exa_column_name" || '" ' || 'decimal(' || case when numeric_precision > 36 then 36 else numeric_precision end || ',' || case when (numeric_scale > numeric_precision) then numeric_precision else  case when numeric_scale < 0 then 0 else numeric_scale end end || ')' end
        /* alternative when you want to keep the value as a double and precision > 36
        when upper(data_type) = 'DECIMAL' then case when numeric_precision is null or numeric_precision > 36 then 'DOUBLE' else 'decimal(' || numeric_precision || ',' || case when (numeric_scale > numeric_precision) then numeric_precision else  case when numeric_scale < 0 then 0 else numeric_scale end end || ')' end
        */
        when upper(data_type) = 'BIT' then '"' || "exa_column_name" || '" ' || 'DECIMAL('||numeric_precision||',0)'

        -- ### date and time types ###
        when upper(data_type) = 'DATE' then '"' || "exa_column_name" || '" ' || 'DATE'
        when upper(data_type) = 'DATETIME' then '"' || "exa_column_name" || '" ' || 'TIMESTAMP'
        when upper(data_type) = 'TIMESTAMP' then '"' || "exa_column_name" || '" ' || 'TIMESTAMP'
        when upper(data_type) = 'TIME' then '"' || "exa_column_name" || '" ' || 'varchar(8)'
        when upper(data_type) = 'YEAR' then '"' || "exa_column_name" || '" ' || 'varchar(4)'

        -- ### string types ###
        when upper(data_type) = 'CHAR' then '"' || "exa_column_name" || '" ' || upper(column_type)
        when upper(data_type) = 'VARCHAR' then '"' || "exa_column_name" || '" ' || upper(column_type)
        when upper(data_type) = 'BINARY' then '"' || "exa_column_name" || '" ' || 'char('||character_maximum_length||')'
        when upper(data_type) = 'VARBINARY' then '"' || "exa_column_name" || '" ' || 'varchar('||character_maximum_length||')'
        when upper(data_type) = 'TINYTEXT' then '"' || "exa_column_name" || '" ' || 'varchar(2000000)'
        when upper(data_type) = 'TEXT' then '"' || "exa_column_name" || '" ' || 'varchar(2000000)'
        when upper(data_type) = 'MEDIUMTEXT' then '"' || "exa_column_name" || '" ' || 'varchar(2000000)'
        when upper(data_type) = 'LONGTEXT' then '"' || "exa_column_name" || '" ' || 'varchar(2000000)'
        when upper(data_type) = 'TINYBLOB' then '"' || "exa_column_name" || '" ' || 'varchar(2000000)'
        when upper(data_type) = 'BLOB' then '"' || "exa_column_name" || '" ' || 'varchar(2000000)'
        when upper(data_type) = 'MEDIUMBLOB' then '"' || "exa_column_name" || '" ' || 'varchar(2000000)'
        when upper(data_type) = 'LONGBLOB' then '"' || "exa_column_name" || '" ' || 'varchar(2000000)'
        when upper(data_type) = 'ENUM' then '"' || "exa_column_name" || '" ' || 'varchar(2000000)'
        when upper(data_type) = 'SET' then '"' || "exa_column_name" || '" ' || 'varchar(2000000)'
        when upper(data_type) = 'JSON' then '"' || "exa_column_name" || '" ' || 'varchar(2000000)'

        -- ### geospatial types ###
        when upper(data_type) = 'GEOMETRY' then '"' || "exa_column_name" || '" ' || upper(column_type)
        when upper(data_type) = 'GEOMETRYCOLLECTION' then '"' || "exa_column_name" || '" ' || upper('geometry')
        when upper(data_type) = 'POINT' then '"' || "exa_column_name" || '" ' || upper('geometry')
        when upper(data_type) = 'MULTIPOINT' then '"' || "exa_column_name" || '" ' || upper('geometry')
        when upper(data_type) = 'LINESTRING' then '"' || "exa_column_name" || '" ' || upper('geometry')
        when upper(data_type) = 'MULTILINESTRING' then upper('geometry')
        when upper(data_type) = 'POLYGON' then '"' || "exa_column_name" || '" ' || upper('geometry')
        when upper(data_type) = 'MULTIPOLYGON' then '"' || "exa_column_name" || '" ' || upper('geometry')

        end
        order by ordinal_position) || ');'

        -- ### unknown types ###
        || group_concat (
               case
               when upper(data_type) not in ('INT', 'INTEGER', 'TINYINT', 'SMALLINT', 'MEDIUMINT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'BIT', 'DATE', 'DATETIME', 'TIMESTAMP', 'TIME', 'YEAR', 'CHAR', 'VARCHAR', 'VARBINARY', 'BINARY', 'TINYTEXT', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT', 'TINYBLOB', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB', 'ENUM', 'SET', 'JSON', 'GEOMETRY', 'GEOMETRYCOLLECTION', 'POINT', 'MULTIPOINT', 'LINESTRING', 'MULTILINESTRING', 'POLYGON', 'MULTIPOLYGON')
               then '--UNKNOWN_DATATYPE: "'|| "exa_column_name" || '" ' || upper(data_type) || ''
               end
        )|| ' 'as sql_create_table
        from vv_mysql_columns  group by "exa_table_catalog","exa_table_schema", "exa_table_name", "mysql_table_schema", "mysql_table_name"
        order by "exa_table_catalog","exa_table_schema","exa_table_name"
    )

    , vv_imports as (
        select "mysql_table_schema" AS mysql_table_schema, "mysql_table_name" AS mysql_table_name,"exa_table_schema" AS exa_table_schema, "exa_table_name" AS exa_table_name, 'import into "' || "exa_table_schema" || '"."' || "exa_table_name" || '" from jdbc at ]]..CONNECTION_NAME..[[ statement ''select '
               || group_concat(
                                case
                                -- ### numeric types ###
                                when upper(data_type) = 'INT' then '`' || column_name || '`'
                                when upper(data_type) = 'INTEGER' then '`' || column_name || '`'
                                when upper(data_type) = 'TINYINT' then '`' || column_name || '`'
                                when upper(data_type) = 'SMALLINT' then '`' || column_name || '`'
                                when upper(data_type) = 'MEDIUMINT' then '`' || column_name || '`'
                                when upper(data_type) = 'BIGINT' then '`' || column_name || '`'
                                when upper(data_type) = 'FLOAT' then '`' || column_name || '`'
                                when upper(data_type) = 'DOUBLE' then '`' || column_name || '`'
                                when upper(data_type) = 'DECIMAL' then '`' || column_name || '`'

                                -- ### date and time types ###
                                when upper(data_type) = 'DATE' then '`' || column_name || '`'
                                when upper(data_type) = 'DATETIME' then '`' || column_name || '`'
                                when upper(data_type) = 'TIMESTAMP' then '`' || column_name || '`'

                                -- ### string types ###
                                when upper(data_type) = 'CHAR' then '`' || column_name || '`'
                                when upper(data_type) = 'VARCHAR' then '`' || column_name || '`'
                                when upper(data_type) = 'TINYTEXT' then '`' || column_name || '`'
                                when upper(data_type) = 'TEXT' then '`' || column_name || '`'
                                when upper(data_type) = 'MEDIUMTEXT' then '`' || column_name || '`'
                                when upper(data_type) = 'LONGTEXT' then '`' || column_name || '`'
                                when upper(data_type) = 'ENUM' then '`' || column_name || '`'
                                when upper(data_type) = 'SET' then '`' || column_name || '`'
                                when upper(data_type) = 'JSON' then '`' || column_name || '`'

                                when upper(data_type) = 'BINARY' then 'cast(`'||column_name||'` as char('||character_maximum_length||'))'
                                when upper(data_type) = 'VARBINARY' then 'cast(`'||column_name||'` as char('||character_maximum_length||'))'
                                when upper(data_type) = 'TINYBLOB' then 'cast(`'||column_name||'` as char(2000000))'
                                when upper(data_type) = 'MEDIUMBLOB' then 'cast(`'||column_name||'` as char(2000000))'
                                when upper(data_type) = 'BLOB' then 'cast(`'||column_name||'` as char(2000000))'
                                when upper(data_type) = 'LONGBLOB' then 'cast(`'||column_name||'` as char(2000000))'
                                when upper(data_type) = 'TIME' then 'cast(`'||column_name||'` as char(8))'
                                when upper(data_type) = 'YEAR' then 'cast(`'||column_name||'` as char(4))'
                                -- ### for MySQL versions below 5.6 ST_AsText() needs to be replaced with AsText() ###
                                when upper(data_type) = 'GEOMETRY' then 'ST_AsText(`'||column_name||'`)'
                                when upper(data_type) = 'GEOMETRYCOLLECTION' then 'ST_AsText(`'||column_name||'`)'
                                when upper(data_type) = 'POINT' then 'ST_AsText(`'||column_name||'`)'
                                when upper(data_type) = 'MULTIPOINT' then 'ST_AsText(`'||column_name||'`)'
                                when upper(data_type) = 'LINESTRING' then 'ST_AsText(`'||column_name||'`)'
                                when upper(data_type) = 'MULTILINESTRING' then 'ST_AsText(`'||column_name||'`)'
                                when upper(data_type) = 'POLYGON' then 'ST_AsText(`'||column_name||'`)'
                                when upper(data_type) = 'MULTIPOLYGON' then 'ST_AsText(`'||column_name||'`)'
                                when upper(data_type) = 'BIT' then 'cast(`'||column_name||'` as DECIMAL('||numeric_precision||',0))'
                                end order by ordinal_position)
               || ' from ' || table_schema|| '.' || table_name|| '''' as sql_imports
        from vv_mysql_columns group by "exa_table_catalog","exa_table_schema","exa_table_name" , "mysql_table_schema", "mysql_table_name", table_schema,table_name
        order by "exa_table_catalog", "exa_table_schema","exa_table_name", table_schema,table_name
    )
    select * from (

    SELECT  a.sql_create_schema
         , b.*
        , c.sql_imports

    from vv_create_tables AS b
    LEFT JOIN vv_imports AS c ON b.mysql_table_schema = c.mysql_table_schema AND b.mysql_table_name = c.mysql_table_name
    LEFT JOIN vv_create_schemas AS a ON b.mysql_table_schema = a.mysql_table_schema AND b.mysql_table_name = a.mysql_table_name


    )
    ]],{})

    if not suc then
      error('"'..res.error_message..'" Caught while executing: "'..res.statement_text..'"')
    end

    return(res)