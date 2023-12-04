#include <rpr.h>
#include <rda.h>

static void zz_display_last_error(struct da_last_error_message_provider *last_error_message_provider, rt_char *prefix)
{
	rt_char buffer[RT_CHAR_BIG_STRING_SIZE];
	rt_un buffer_size;

	buffer_size = rt_char_get_size(prefix);
	if (RT_UNLIKELY(!rt_char_copy(prefix, buffer_size, buffer, RT_CHAR_BIG_STRING_SIZE))) goto the_end;
	if (RT_UNLIKELY(!rt_char_append(_R(" - "), 3, buffer, RT_CHAR_BIG_STRING_SIZE, &buffer_size))) goto the_end;
	if (RT_UNLIKELY(!last_error_message_provider->append(last_error_message_provider, buffer, RT_CHAR_BIG_STRING_SIZE, &buffer_size))) goto the_end;

	rt_console_write_error_with_size(buffer, buffer_size);

the_end:
	return;
}

static rt_s zz_execute_statement(rt_char8 *sql, struct da_statement *statement, struct da_connection *connection, rt_b ignore_errors)
{
	rt_char buffer[RT_CHAR_HALF_BIG_STRING_SIZE];
	rt_un buffer_size;
	rt_char *output;
	rt_s ret;

	/* Display the SQL command. */
	if (RT_UNLIKELY(!rt_encoding_decode(sql, rt_char8_get_size(sql), RT_ENCODING_SYSTEM_DEFAULT, buffer, RT_CHAR_HALF_BIG_STRING_SIZE, RT_NULL, RT_NULL, &output, &buffer_size, RT_NULL))) goto error;
	if (RT_UNLIKELY(!rt_char_append_char(_R('\n'), buffer, RT_CHAR_HALF_BIG_STRING_SIZE, &buffer_size))) goto error;
	if (RT_UNLIKELY(!rt_console_write_string_with_size(buffer, buffer_size))) goto error;

	if (RT_UNLIKELY(!statement->execute(statement, sql) && !ignore_errors)) {
		zz_display_last_error(&connection->last_error_message_provider, _R("Statement execution failed"));
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s zz_test_statement(struct da_statement *statement, struct da_connection *connection)
{
	rt_un row_count;
	rt_s ret;

	if (RT_UNLIKELY(!zz_execute_statement("drop table RDA_TESTS_TABLE", statement, connection, RT_TRUE))) goto error;
	if (RT_UNLIKELY(!zz_execute_statement("create table RDA_TESTS_TABLE (VAL varchar2(200))", statement, connection, RT_FALSE))) goto error;

	if (RT_UNLIKELY(!zz_execute_statement("insert into RDA_TESTS_TABLE values ('1')", statement, connection, RT_FALSE))) goto error;
	if (RT_UNLIKELY(!zz_execute_statement("insert into RDA_TESTS_TABLE values ('2')", statement, connection, RT_FALSE))) goto error;
	if (RT_UNLIKELY(!zz_execute_statement("insert into RDA_TESTS_TABLE values ('2')", statement, connection, RT_FALSE))) goto error;

	if (RT_UNLIKELY(!connection->commit(connection))) goto error;

	if (RT_UNLIKELY(!zz_execute_statement("insert into RDA_TESTS_TABLE values ('2')", statement, connection, RT_FALSE))) goto error;

	if (RT_UNLIKELY(!connection->rollback(connection))) goto error;

	if (RT_UNLIKELY(!zz_execute_statement("update RDA_TESTS_TABLE set VAL = '3' where VAL = '2'", statement, connection, RT_FALSE))) goto error;
	if (RT_UNLIKELY(!statement->get_row_count(statement, &row_count))) goto error;
	if (RT_UNLIKELY(row_count != 2)) goto error;

	if (RT_UNLIKELY(!connection->commit(connection))) goto error;

	if (RT_UNLIKELY(!zz_execute_statement("insert into RDA_TESTS_TABLE values ('333')", statement, connection, RT_FALSE))) goto error;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s zz_test_connection(struct da_connection *connection)
{
	struct da_statement statement;
	rt_b statement_created = RT_FALSE;
	rt_s ret;

	if (RT_UNLIKELY(!connection->open(connection))) {
		zz_display_last_error(&connection->last_error_message_provider, _R("Failed to open the connection"));
		goto error;
	}

	if (RT_UNLIKELY(!rt_console_write_string(_R("Connected.\n"))))
		goto error;

	if (RT_UNLIKELY(!connection->create_statement(connection, &statement)))
		goto error;
	statement_created = RT_TRUE;

	if (RT_UNLIKELY(!zz_test_statement(&statement, connection)))
		goto error;

	ret = RT_OK;
free:
	if (statement_created) {
		statement_created = RT_FALSE;
		if (RT_UNLIKELY(!statement.free(&statement) && ret))
			goto error;
	}
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s zz_test_data_source(struct da_data_source *data_source)
{
	struct da_connection connection;
	rt_b connection_created = RT_FALSE;
	rt_s ret;

	if (RT_UNLIKELY(!data_source->open(data_source))) {
		zz_display_last_error(&data_source->last_error_message_provider, _R("Failed to connect to the database"));
		goto error;
	}

	if (RT_UNLIKELY(!data_source->create_connection(data_source, &connection, RT_FALSE)))
		goto error;
	connection_created = RT_TRUE;

	if (!zz_test_connection(&connection))
		goto error;

	ret = RT_OK;
free:
	if (connection_created) {
		connection_created = RT_FALSE;
		if (RT_UNLIKELY(!connection.free(&connection) && ret))
			goto error;
	}
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s zz_test_driver(struct da_driver *driver, const rt_char8 *host_name, rt_un port, const rt_char8 *database, const rt_char8 *user_name, const rt_char8 *password)
{
	struct da_data_source data_source;
	rt_b data_source_created = RT_FALSE;
	rt_s ret;

	if (RT_UNLIKELY(!driver->create_data_source(driver, &data_source, host_name, port, database, user_name, password)))
		goto error;
	data_source_created = RT_TRUE;

	if (RT_UNLIKELY(!zz_test_data_source(&data_source)))
		goto error;

	ret = RT_OK;
free:
	if (data_source_created) {
		data_source_created = RT_FALSE;
		if (RT_UNLIKELY(!data_source.free(&data_source) && ret))
			goto error;
	}
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s zz_test_rda(const rt_char8 *host_name, rt_un port, const rt_char8 *database, const rt_char8 *user_name, const rt_char8 *password)
{
	struct da_driver driver;
	rt_b driver_created = RT_FALSE;
	rt_s ret;

	if (RT_UNLIKELY(!rt_console_write_string(_R("Connecting...\n"))))
		goto error;

	if (RT_UNLIKELY(!da_oracle_driver_create(&driver)))
		goto error;
	driver_created = RT_TRUE;

	if (RT_UNLIKELY(!zz_test_driver(&driver, host_name, port, database, user_name, password)))
		goto error;

	ret = RT_OK;
free:
	if (driver_created) {
		driver_created = RT_FALSE;
		if (RT_UNLIKELY(!driver.free(&driver) && ret))
			goto error;
	}
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}
