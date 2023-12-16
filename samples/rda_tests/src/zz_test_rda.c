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

static rt_s zz_execute_statement(rt_char8 *sql, struct da_statement *statement, rt_b ignore_errors, rt_un *row_count)
{
	rt_char buffer[RT_CHAR_HALF_BIG_STRING_SIZE];
	rt_un buffer_size;
	rt_char *output;
	rt_s ret;

	/* Display the SQL command. */
	if (RT_UNLIKELY(!rt_encoding_decode(sql, rt_char8_get_size(sql), RT_ENCODING_SYSTEM_DEFAULT, buffer, RT_CHAR_HALF_BIG_STRING_SIZE, RT_NULL, RT_NULL, &output, &buffer_size, RT_NULL))) goto error;
	if (RT_UNLIKELY(!rt_char_append_char(_R('\n'), buffer, RT_CHAR_HALF_BIG_STRING_SIZE, &buffer_size))) goto error;
	if (RT_UNLIKELY(!rt_console_write_string_with_size(buffer, buffer_size))) goto error;

	if (RT_UNLIKELY(!statement->execute(statement, sql, row_count) && !ignore_errors)) {
		zz_display_last_error(&statement->last_error_message_provider, _R("Statement execution failed"));
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s zz_test_execute_with_statement(struct da_statement *statement, struct da_connection *connection, enum da_database_type database_type)
{
	rt_un row_count;
	rt_s ret;

	switch (database_type) {
	case DA_DATABASE_TYPE_ORACLE:
		if (RT_UNLIKELY(!zz_execute_statement("drop table RDA_TESTS_TABLE", statement, RT_TRUE, RT_NULL))) goto error;
		if (RT_UNLIKELY(!zz_execute_statement("create table RDA_TESTS_TABLE (VAL1 varchar2(200), VAL2 varchar2(200), VAL3 number(15))", statement, RT_FALSE, RT_NULL))) goto error;
		break;
	case DA_DATABASE_TYPE_POSTGRES:
		if (RT_UNLIKELY(!zz_execute_statement("drop table if exists RDA_TESTS_TABLE", statement, RT_FALSE, RT_NULL))) goto error;
		if (RT_UNLIKELY(!zz_execute_statement("create table RDA_TESTS_TABLE (VAL1 varchar(200), VAL2 varchar(200), VAL3 numeric(15))", statement, RT_FALSE, RT_NULL))) goto error;
		break;
	}

	if (RT_UNLIKELY(!zz_execute_statement("insert into RDA_TESTS_TABLE values ('1', '', 1)", statement, RT_FALSE, RT_NULL))) goto error;
	if (RT_UNLIKELY(!zz_execute_statement("insert into RDA_TESTS_TABLE values ('2', null, 2)", statement, RT_FALSE, RT_NULL))) goto error;
	if (RT_UNLIKELY(!zz_execute_statement("insert into RDA_TESTS_TABLE values ('2', null, 2)", statement, RT_FALSE, RT_NULL))) goto error;

	if (RT_UNLIKELY(!connection->commit(connection))) goto error;

	if (RT_UNLIKELY(!zz_execute_statement("insert into RDA_TESTS_TABLE values ('2', null, 2)", statement, RT_FALSE, RT_NULL))) goto error;

	if (RT_UNLIKELY(!connection->rollback(connection))) goto error;

	if (RT_UNLIKELY(!zz_execute_statement("update RDA_TESTS_TABLE set VAL1 = '3' where VAL1 = '2'", statement, RT_FALSE, &row_count))) goto error;
	if (RT_UNLIKELY(row_count != 2)) goto error;

	if (RT_UNLIKELY(!connection->commit(connection))) goto error;

	if (RT_UNLIKELY(!zz_execute_statement("insert into RDA_TESTS_TABLE values ('333', null, 333)", statement, RT_FALSE, RT_NULL))) goto error;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s zz_test_execute_with_connection(struct da_connection *connection, enum da_database_type database_type)
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

	if (RT_UNLIKELY(!zz_test_execute_with_statement(&statement, connection, database_type)))
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

static rt_s zz_test_query_with_statement(struct da_statement *statement, enum da_database_type database_type)
{
	struct da_result result;
	rt_b result_created = RT_FALSE;
	struct da_binding bindings[3];
	rt_char8 buffer1[256];
	rt_char8 buffer2[256];
	rt_b no_more_rows;
	rt_un line = 0;
	const rt_char8 *val1;
	rt_n32 val3;
	rt_s ret;

	if (RT_UNLIKELY(!statement->create_result(statement, &result, "select VAL1, VAL2, VAL3 from RDA_TESTS_TABLE order by VAL1"))) {
		zz_display_last_error(&result.last_error_message_provider, _R("Select failed"));
		goto error;
	}
	result_created = RT_TRUE;

	/* VAL1. */
	bindings[0].type = DA_BINDING_TYPE_CHAR8;
	bindings[0].u.char8.buffer = buffer1;
	bindings[0].u.char8.buffer_capacity = 256;
	RT_MEMORY_SET(buffer1, 'x', sizeof(buffer1));

	/* VAL2. */
	bindings[1].type = DA_BINDING_TYPE_CHAR8;
	bindings[1].u.char8.buffer = buffer2;
	bindings[1].u.char8.buffer_capacity = 256;
	RT_MEMORY_SET(buffer2, 'x', sizeof(buffer2));

	/* VAL3. */
	bindings[2].type = DA_BINDING_TYPE_N32;

	if (RT_UNLIKELY(!result.bind(&result, bindings, sizeof(bindings) / sizeof(bindings[0])))) {
		zz_display_last_error(&result.last_error_message_provider, _R("Binding failed"));
		goto error;
	}

	while (RT_TRUE) {
		if (RT_UNLIKELY(!result.fetch(&result, &no_more_rows))) {
			zz_display_last_error(&result.last_error_message_provider, _R("Fetch failed"));
			goto error;
		}

		if (no_more_rows)
			break;

		line++;

		if (line == 1) {
			val1 = "1";
			val3 = 1;
		} else {
			val1 = "3";
			val3 = 2;
		}

		/* Check VAL1. */
		if (RT_UNLIKELY(bindings[0].is_null)) goto error;
		if (RT_UNLIKELY(rt_char8_get_size(buffer1) != 1)) goto error;
		if (RT_UNLIKELY(bindings[0].u.char8.buffer_size != 1)) goto error;
		if (RT_UNLIKELY(!rt_char8_equals(buffer1, 1, val1, rt_char8_get_size(val1)))) goto error;

		/* Check VAL2. */
		if (line == 1 && database_type != DA_DATABASE_TYPE_ORACLE) {
			if (RT_UNLIKELY(bindings[1].is_null)) goto error;
			if (RT_UNLIKELY(rt_char8_get_size(buffer2) != 0)) goto error;
			if (RT_UNLIKELY(bindings[1].u.char8.buffer_size != 0)) goto error;
		} else {
			if (RT_UNLIKELY(!bindings[1].is_null)) goto error;
		}

		/* Check VAL3. */
		if (RT_UNLIKELY(bindings[2].is_null)) goto error;
		if (RT_UNLIKELY(bindings[2].u.n32.value != val3)) goto error;
	}
	if (RT_UNLIKELY(line != 3))
		goto error;

	ret = RT_OK;
free:
	if (result_created) {
		result_created = RT_FALSE;
		if (RT_UNLIKELY(!result.free(&result) && ret))
			goto error;
	}
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s zz_test_query_with_connection(struct da_connection *connection, enum da_database_type database_type)
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

	if (RT_UNLIKELY(!zz_test_query_with_statement(&statement, database_type)))
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

static rt_s zz_test_data_source(struct da_data_source *data_source, enum da_database_type database_type)
{
	struct da_connection execute_connection;
	rt_b execute_connection_created = RT_FALSE;
	struct da_connection query_connection;
	rt_b query_connection_created = RT_FALSE;
	rt_s ret;

	if (RT_UNLIKELY(!data_source->open(data_source))) {
		zz_display_last_error(&data_source->last_error_message_provider, _R("Failed to connect to the database"));
		goto error;
	}

	if (RT_UNLIKELY(!data_source->create_connection(data_source, &execute_connection, RT_FALSE)))
		goto error;
	execute_connection_created = RT_TRUE;

	if (RT_UNLIKELY(!zz_test_execute_with_connection(&execute_connection, database_type)))
		goto error;

	execute_connection_created = RT_FALSE;
	if (RT_UNLIKELY(!execute_connection.free(&execute_connection)))
		goto error;

	if (RT_UNLIKELY(!data_source->create_connection(data_source, &query_connection, RT_FALSE)))
		goto error;
	query_connection_created = RT_TRUE;

	if (RT_UNLIKELY(!zz_test_query_with_connection(&query_connection, database_type)))
		goto error;

	ret = RT_OK;
free:
	if (query_connection_created) {
		query_connection_created = RT_FALSE;
		if (RT_UNLIKELY(!query_connection.free(&query_connection) && ret))
			goto error;
	}

	if (execute_connection_created) {
		execute_connection_created = RT_FALSE;
		if (RT_UNLIKELY(!execute_connection.free(&execute_connection) && ret))
			goto error;
	}

	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s zz_test_driver(struct da_driver *driver, const rt_char8 *host_name, rt_un port, const rt_char8 *database, const rt_char8 *user_name, const rt_char8 *password, enum da_database_type database_type)
{
	struct da_data_source data_source;
	rt_b data_source_created = RT_FALSE;
	rt_s ret;

	if (RT_UNLIKELY(!driver->create_data_source(driver, &data_source, host_name, port, database, user_name, password)))
		goto error;
	data_source_created = RT_TRUE;

	if (RT_UNLIKELY(!zz_test_data_source(&data_source, database_type)))
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

rt_s zz_test_rda(const rt_char8 *host_name, rt_un port, const rt_char8 *database, const rt_char8 *user_name, const rt_char8 *password, enum da_database_type database_type)
{
	struct da_driver driver;
	rt_b driver_created = RT_FALSE;
	rt_s ret;

	if (RT_UNLIKELY(!rt_console_write_string(_R("Connecting...\n"))))
		goto error;

	if (RT_UNLIKELY(!da_driver_manager_create_driver(&driver, database_type)))
		goto error;
	driver_created = RT_TRUE;

	if (RT_UNLIKELY(!zz_test_driver(&driver, host_name, port, database, user_name, password, database_type)))
		goto error;

	if (RT_UNLIKELY(!rt_console_write_string(_R("Tests successful.\n"))))
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
