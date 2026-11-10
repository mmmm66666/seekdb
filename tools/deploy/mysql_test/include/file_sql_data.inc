## Deploy the static data used by the file_sql suite.
## mysqltest and the observer run on the same host in the CI runner, so the data is
## copied to a fixed absolute path instead of being transferred with ssh/scp.
--disable_query_log
--exec rm -rf /tmp/file_sql_data
--exec cp -r mysql_test/test_suite/file_sql/data /tmp/file_sql_data
--enable_query_log