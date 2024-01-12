#include "layer004/da_mssql_connection.h"

#include "layer000/da_mssql_headers.h"
#include "layer001/da_mssql_utils.h"
#include "layer003/da_mssql_statement.h"

rt_s da_mssql_connection_open(struct da_connection *connection)
{
	struct da_data_source *data_source = connection->data_source;
	struct da_driver *driver = data_source->driver;
	SQLHENV environment_handle = driver->u.mssql.environment_handle;
	SQLHDBC connection_handle;
	rt_char8 *connection_string = data_source->u.mssql.connection_string;
	SQLRETURN status;
	rt_b connected = RT_FALSE;
	rt_s ret;

	if (RT_UNLIKELY(connection->opened)) {
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	status = SQLAllocHandle(SQL_HANDLE_DBC, environment_handle, &connection_handle);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	connection->u.mssql.connection_handle = connection_handle;
	connection->u.mssql.connection_handle_created = RT_TRUE;

	if (connection->auto_commit) {
		status = SQLSetConnectAttr(connection_handle, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, SQL_IS_UINTEGER);
	} else {
		status = SQLSetConnectAttr(connection_handle, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, SQL_IS_UINTEGER);
	}
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		da_mssql_utils_set_with_last_error(SQL_HANDLE_DBC, connection_handle);
		goto error;
	}

	status = SQLDriverConnect(connection_handle, RT_NULL, (SQLCHAR*)connection_string, SQL_NTS, RT_NULL, 0, RT_NULL, SQL_DRIVER_NOPROMPT);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		da_mssql_utils_set_with_last_error(SQL_HANDLE_DBC, connection_handle);
		goto error;
	}
	connected = RT_TRUE;


	connection->opened = RT_TRUE;

	ret = RT_OK;
free:
	return ret;

error:
	if (connected) {
		SQLDisconnect(connection_handle);
	}

	ret = RT_FAILED;
	goto free;
}

rt_s da_mssql_connection_create_statement(struct da_connection *connection, struct da_statement *statement)
{
	SQLHDBC connection_handle = connection->u.mssql.connection_handle;
	SQLHSTMT statement_handle;
	SQLRETURN status;
	rt_s ret;

	if (RT_UNLIKELY(!connection->opened)) {
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	statement->execute = &da_mssql_statement_execute;
	statement->select = &da_mssql_statement_select;
	statement->execute_prepared = &da_mssql_statement_execute_prepared;
	statement->select_prepared = &da_mssql_statement_select_prepared;
	statement->free = &da_mssql_statement_free;

	statement->connection = connection;

	statement->prepared_sql = RT_NULL;
	statement->prepared = RT_FALSE;

	status = SQLAllocHandle(SQL_HANDLE_STMT, connection_handle, &statement_handle);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	statement->u.mssql.statement_handle = statement_handle;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_mssql_connection_prepare_statement(struct da_connection *connection, struct da_statement *statement, const rt_char8 *sql)
{
	rt_s ret;

	if (RT_UNLIKELY(!da_mssql_connection_create_statement(connection, statement)))
		goto error;

	statement->prepared_sql = sql;

	statement->u.mssql.cumulative_row_count = 0;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_mssql_connection_commit(struct da_connection *connection)
{
	SQLHDBC connection_handle = connection->u.mssql.connection_handle;
	SQLRETURN status;
	rt_s ret;

	status = SQLEndTran(SQL_HANDLE_DBC, connection_handle, SQL_COMMIT);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		da_mssql_utils_set_with_last_error(SQL_HANDLE_DBC, connection_handle);
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_mssql_connection_rollback(struct da_connection *connection)
{
	SQLHDBC connection_handle = connection->u.mssql.connection_handle;
	SQLRETURN status;
	rt_s ret;

	status = SQLEndTran(SQL_HANDLE_DBC, connection_handle, SQL_ROLLBACK);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		da_mssql_utils_set_with_last_error(SQL_HANDLE_DBC, connection_handle);
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_mssql_connection_free(struct da_connection *connection)
{
	SQLHDBC connection_handle = connection->u.mssql.connection_handle;
	SQLRETURN status;
	rt_s ret = RT_OK;

	if (connection->opened) {

		if (!connection->auto_commit) {
			if (RT_UNLIKELY(!da_mssql_connection_rollback(connection)))
				ret = RT_FAILED;
		}

		status = SQLDisconnect(connection_handle);
		if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
			rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
			rt_last_error_message_set_with_last_error();
			ret = RT_FAILED;
		}
	}

	if (connection->u.mssql.connection_handle_created) {
		status = SQLFreeHandle(SQL_HANDLE_DBC, connection_handle);
		if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
			rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
			rt_last_error_message_set_with_last_error();
			ret = RT_FAILED;
		}
	}

	return ret;
}
