## Remove the data deployed by file_sql_data.inc.
--disable_query_log
--exec rm -rf /tmp/file_sql_data
--enable_query_log