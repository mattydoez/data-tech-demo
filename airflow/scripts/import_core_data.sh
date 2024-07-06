#!/bin/bash

table_name=$1
id_col=$2
col_list=$3
company_db_conn=$4
company_dw_conn=$5

LAST_TRANSFER=$(psql $company_dw_conn -t -c "SELECT COALESCE((SELECT last_transfer FROM ops.last_transfer WHERE table_name = '$table_name' ORDER BY last_transfer DESC LIMIT 1), '1970-01-01 00:00:00')")

# Export data from the source database to stdout and pipe it to the destination database
psql $company_dw_conn -c "CREATE TABLE IF NOT EXISTS temp_$table_name (LIKE raw.$table_name INCLUDING DEFAULTS);"
psql $company_db_conn -c "\copy (SELECT * FROM $table_name WHERE created_at > '$LAST_TRANSFER') TO stdout" | psql $company_dw_conn -c "\copy temp_$table_name FROM stdin;"
psql $company_dw_conn -c "INSERT INTO raw.$table_name($col_list)
                                SELECT $col_list
                                FROM temp_$table_name
                                WHERE temp_$table_name.$id_col NOT IN (SELECT $id_col FROM raw.$table_name)
                                ON CONFLICT ($id_col) DO NOTHING;
                                DROP TABLE temp_$table_name;"