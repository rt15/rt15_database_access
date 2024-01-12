#include <rpr.h>
#include <rda_mssql.h>
#include <rda_oracle.h>
#include <rda_postgres.h>

static rt_s zz_create_driver(struct da_driver *driver, enum da_database_type database_type)
{
	const rt_char *database_type_name;
	rt_s ret;

	switch (database_type) {
	case DA_DATABASE_TYPE_MSSQL:
		database_type_name = _R("MSSQL\n");
		if (RT_UNLIKELY(!da_mssql_driver_create(driver)))
			goto error;
		break;
	case DA_DATABASE_TYPE_ORACLE:
		database_type_name = _R("ORACLE\n");
		if (RT_UNLIKELY(!da_oracle_driver_create(driver)))
			goto error;
		break;
	case DA_DATABASE_TYPE_POSTGRES:
		database_type_name = _R("POSTGRES\n");
		if (RT_UNLIKELY(!da_postgres_driver_create(driver)))
			goto error;
		break;
	default:
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		goto error;
	}

	if (RT_UNLIKELY(!rt_console_write_string(database_type_name)))
		goto error;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
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
		rt_last_error_message_write(_R("Statement execution failed"));
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
	case DA_DATABASE_TYPE_MSSQL:
	case DA_DATABASE_TYPE_POSTGRES:
		if (RT_UNLIKELY(!zz_execute_statement("drop table if exists RDA_TESTS_TABLE", statement, RT_FALSE, RT_NULL))) goto error;
		if (RT_UNLIKELY(!zz_execute_statement("create table RDA_TESTS_TABLE (VAL1 varchar(200), VAL2 varchar(200), VAL3 numeric(15))", statement, RT_FALSE, RT_NULL))) goto error;
		break;
	case DA_DATABASE_TYPE_ORACLE:
		if (RT_UNLIKELY(!zz_execute_statement("drop table RDA_TESTS_TABLE", statement, RT_TRUE, RT_NULL))) goto error;
		if (RT_UNLIKELY(!zz_execute_statement("create table RDA_TESTS_TABLE (VAL1 varchar2(200), VAL2 varchar2(200), VAL3 number(15))", statement, RT_FALSE, RT_NULL))) goto error;
		break;
	}

	if (RT_UNLIKELY(!connection->commit(connection))) goto error;

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
		rt_last_error_message_write(_R("Failed to open the connection"));
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

static rt_s zz_test_execute_prepared(struct da_connection *connection, enum da_database_type database_type)
{
	const rt_char8 *sql;
	struct da_statement statement;
	rt_b statement_created = RT_FALSE;
	enum da_binding_type binding_types[3];
	void *batch1[3];
	void *batch2[3];
	void *batch3[3];
	void *batch4[3];
	void *batch5[3];
	void **batches[4];
	rt_n32 value1 = 4;
	rt_n32 value2 = 5;
	rt_n32 value3 = 6;
	rt_n32 value4 = 7;
	rt_n32 value5 = 8;
	rt_un row_count;
	rt_s ret;

	switch (database_type) {
	case DA_DATABASE_TYPE_MSSQL:
		sql = "insert into RDA_TESTS_TABLE values (?, ?, ?)";
		break;
	case DA_DATABASE_TYPE_ORACLE:
		sql = "insert into RDA_TESTS_TABLE values (:1, :2, :3)";
		break;
	case DA_DATABASE_TYPE_POSTGRES:
		sql = "insert into RDA_TESTS_TABLE values ($1, $2, $3)";
		break;
	default:
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
	}

	if (RT_UNLIKELY(!connection->prepare_statement(connection, &statement, sql))) {
		rt_last_error_message_write(_R("Failed to prepare statement"));
		goto error;
	}
	statement_created = RT_TRUE;

	binding_types[0] = DA_BINDING_TYPE_CHAR8;
	binding_types[1] = DA_BINDING_TYPE_CHAR8;
	binding_types[2] = DA_BINDING_TYPE_N32;

	batches[0] = batch1;
	batches[1] = batch2;
	batches[2] = batch3;
	batches[3] = batch4;

	batch1[0] = "333";
	batch1[1] = RT_NULL;
	batch1[2] = &value1;

	batch2[0] = "4444";
	batch2[1] = RT_NULL;
	batch2[2] = &value2;

	batch3[0] = "55555";
	batch3[1] = RT_NULL;
	batch3[2] = &value3;

	batch4[0] = RT_NULL;
	batch4[1] = RT_NULL;
	batch4[2] = &value4;

	if (RT_UNLIKELY(!statement.execute_prepared(&statement, binding_types, 3, batches, 4, &row_count))) {
		rt_last_error_message_write(_R("Failed to execute prepared statement"));
		goto error;
	}

	if (RT_UNLIKELY(row_count != 4))
		goto error;

	batches[0] = batch5;

	batch5[0] = "8";
	batch5[1] = RT_NULL;
	batch5[2] = &value5;

	if (RT_UNLIKELY(!statement.execute_prepared(&statement, binding_types, 3, batches, 1, &row_count))) {
		rt_last_error_message_write(_R("Failed to execute prepared statement"));
		goto error;
	}

	if (RT_UNLIKELY(row_count != 1))
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

static rt_s zz_test_auto_commit(struct da_connection *connection)
{
	struct da_statement statement;
	rt_b statement_created;
	rt_s ret;

	if (RT_UNLIKELY(!connection->create_statement(connection, &statement)))
		goto error;

	if (RT_UNLIKELY(!zz_execute_statement("insert into RDA_TESTS_TABLE values ('9', null, 9)", &statement, RT_FALSE, RT_NULL)))
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

static rt_s zz_test_select_with_statement(struct da_statement *statement, enum da_database_type database_type)
{
	struct da_result result;
	rt_b result_created = RT_FALSE;
	struct da_binding bindings[3];
	rt_char8 buffer1[256];
	rt_char8 buffer2[256];
	rt_b end_of_rows;
	rt_un line = 0;
	const rt_char8 *val1;
	rt_n32 val3;
	rt_s ret;

	if (RT_UNLIKELY(!statement->select(statement, &result, "select VAL1, VAL2, VAL3 from RDA_TESTS_TABLE order by VAL3"))) {
		rt_last_error_message_write(_R("Select failed"));
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
		rt_last_error_message_write(_R("Binding failed"));
		goto error;
	}

	while (RT_TRUE) {
		if (RT_UNLIKELY(!result.fetch(&result, &end_of_rows))) {
			rt_last_error_message_write(_R("Fetch failed"));
			goto error;
		}

		if (end_of_rows)
			break;

		line++;

		switch (line) {
		case 1:
			val1 = "1";
			val3 = 1;
			break;
		case 2:
		case 3:
			val1 = "3";
			val3 = 2;
			break;
		case 4:
			val1 = "333";
			val3 = 4;
			break;
		case 5:
			val1 = "4444";
			val3 = 5;
			break;
		case 6:
			val1 = "55555";
			val3 = 6;
			break;
		case 7:
			val1 = RT_NULL;
			val3 = 7;
			break;
		case 8:
			val1 = "8";
			val3 = 8;
			break;
		case 9:
			val1 = "9";
			val3 = 9;
			break;
		default:
			rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
			goto error;
		}

		/* Check VAL1. */
		if (val1) {
			if (RT_UNLIKELY(bindings[0].is_null)) goto error;
			if (RT_UNLIKELY(bindings[0].u.char8.buffer_size != rt_char8_get_size(buffer1))) goto error;
			if (RT_UNLIKELY(!rt_char8_equals(buffer1, rt_char8_get_size(buffer1), val1, rt_char8_get_size(val1)))) goto error;
		} else {
			if (RT_UNLIKELY(!bindings[0].is_null)) goto error;
		}

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
	if (RT_UNLIKELY(line != 9))
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

static rt_s zz_test_select_with_prepared_statement(struct da_statement *statement)
{
	enum da_binding_type binding_types[2] = { DA_BINDING_TYPE_CHAR8, DA_BINDING_TYPE_N32 };
	rt_n32 val3 = 4;
	void *select_bindings[2];
	struct da_result result;
	rt_b result_created = RT_FALSE;
	struct da_binding bindings[3];
	rt_char8 buffer1[256];
	rt_char8 buffer2[256];
	rt_un lines_count = 0;
	rt_b end_of_rows;
	rt_s ret;

	select_bindings[0] = "333";
	select_bindings[1] = &val3;

	if (RT_UNLIKELY(!statement->select_prepared(statement, &result, binding_types, 2, select_bindings))) {
		rt_last_error_message_write(_R("Prepared statement execution failed"));
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

	if (RT_UNLIKELY(!result.bind(&result, bindings, 3))) {
		rt_last_error_message_write(_R("Binding failed"));
		goto error;
	}

	while (RT_TRUE) {
		if (RT_UNLIKELY(!result.fetch(&result, &end_of_rows))) {
			rt_last_error_message_write(_R("Fetch failed"));
			goto error;
		}

		if (end_of_rows)
			break;

		lines_count++;

		/* VAL1. */
		if (RT_UNLIKELY(bindings[0].is_null)) goto error;
		if (RT_UNLIKELY(bindings[0].u.char8.buffer_size != rt_char8_get_size(buffer1))) goto error;
		if (RT_UNLIKELY(!rt_char8_equals(buffer1, rt_char8_get_size(buffer1), "333", 3))) goto error;

		/* VAL2. */
		if (RT_UNLIKELY(!bindings[1].is_null)) goto error;

		/* VAL3. */
		if (RT_UNLIKELY(bindings[2].is_null)) goto error;
		if (RT_UNLIKELY(bindings[2].u.n32.value != 4)) goto error;
	}

	if (RT_UNLIKELY(lines_count != 1))
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

static rt_s zz_test_select_with_connection(struct da_connection *connection, enum da_database_type database_type)
{
	rt_char8 *sql;
	struct da_statement statement;
	rt_b statement_created = RT_FALSE;
	struct da_statement prepared_statement;
	rt_b prepared_statement_created = RT_FALSE;
	rt_s ret;

	switch (database_type) {
	case DA_DATABASE_TYPE_MSSQL:
		sql = "select VAL1, VAL2, VAL3 from RDA_TESTS_TABLE where VAL1 = ? and VAL3 = ?";
		break;
	case DA_DATABASE_TYPE_ORACLE:
		sql = "select VAL1, VAL2, VAL3 from RDA_TESTS_TABLE where VAL1 = :1 and VAL3 = :2";
		break;
	case DA_DATABASE_TYPE_POSTGRES:
		sql = "select VAL1, VAL2, VAL3 from RDA_TESTS_TABLE where VAL1 = $1 and VAL3 = $2";
		break;
	default:
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
	}

	if (RT_UNLIKELY(!connection->open(connection))) {
		rt_last_error_message_write(_R("Failed to open the connection"));
		goto error;
	}

	if (RT_UNLIKELY(!rt_console_write_string(_R("Connected.\n"))))
		goto error;

	if (RT_UNLIKELY(!connection->create_statement(connection, &statement)))
		goto error;
	statement_created = RT_TRUE;

	if (RT_UNLIKELY(!zz_test_select_with_statement(&statement, database_type)))
		goto error;

	if (RT_UNLIKELY(!connection->prepare_statement(connection, &prepared_statement, sql)))
		goto error;
	prepared_statement_created = RT_TRUE;

	if (RT_UNLIKELY(!zz_test_select_with_prepared_statement(&prepared_statement)))
		goto error;

	ret = RT_OK;
free:
	if (prepared_statement_created) {
		prepared_statement_created = RT_FALSE;
		if (RT_UNLIKELY(!prepared_statement.free(&prepared_statement) && ret))
			goto error;
	}
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
	struct da_connection auto_commit_connection;
	rt_b auto_commit_connection_created = RT_FALSE;
	struct da_connection select_connection;
	rt_b select_connection_created = RT_FALSE;
	rt_s ret;

	if (RT_UNLIKELY(!data_source->open(data_source))) {
		rt_last_error_message_write(_R("Failed to connect to the database"));
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

	if (RT_UNLIKELY(!data_source->create_connection(data_source, &auto_commit_connection, RT_TRUE)))
		goto error;
	auto_commit_connection_created = RT_TRUE;

	if (RT_UNLIKELY(!auto_commit_connection.open(&auto_commit_connection)))
		goto error;

	if (RT_UNLIKELY(!zz_test_execute_prepared(&auto_commit_connection, database_type)))
		goto error;

	if (RT_UNLIKELY(!zz_test_auto_commit(&auto_commit_connection)))
		goto error;

	if (RT_UNLIKELY(!data_source->create_connection(data_source, &select_connection, RT_FALSE)))
		goto error;
	select_connection_created = RT_TRUE;

	if (RT_UNLIKELY(!zz_test_select_with_connection(&select_connection, database_type)))
		goto error;

	ret = RT_OK;
free:
	if (select_connection_created) {
		select_connection_created = RT_FALSE;
		if (RT_UNLIKELY(!select_connection.free(&select_connection) && ret))
			goto error;
	}

	if (auto_commit_connection_created) {
		auto_commit_connection_created = RT_FALSE;
		if (RT_UNLIKELY(!auto_commit_connection.free(&auto_commit_connection) && ret))
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

	if (RT_UNLIKELY(!zz_create_driver(&driver, database_type)))
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
