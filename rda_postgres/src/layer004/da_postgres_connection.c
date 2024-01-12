#include "layer004/da_postgres_connection.h"

#include "layer000/da_postgres_headers.h"
#include "layer001/da_postgres_utils.h"
#include "layer003/da_postgres_statement.h"

static rt_s da_postgres_connection_execute(struct da_connection *connection, const rt_char8 *sql)
{
	PGconn *pg_conn = connection->u.postgres.pg_conn;
	PGresult *pg_result = RT_NULL;
	rt_s ret;

	pg_result = PQexec(pg_conn, sql);
	if (RT_UNLIKELY(!pg_result)) {
		da_postgres_utils_set_with_last_error(connection);
		goto error;
	}
	if (RT_UNLIKELY(PQresultStatus(pg_result) != PGRES_COMMAND_OK)) {
		da_postgres_utils_set_with_last_error(connection);
		goto error;
	}

	ret = RT_OK;
free:
	if (pg_result) {
		/* PQClear returns void. */
		PQclear(pg_result);
	}
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_connection_open(struct da_connection *connection)
{
	struct da_data_source *data_source = connection->data_source;
	PGconn *pg_conn = RT_NULL;
	rt_s ret;

	if (RT_UNLIKELY(connection->opened)) {
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	pg_conn = PQconnectdbParams(data_source->u.postgres.keywords, data_source->u.postgres.values, 1);
	if (RT_UNLIKELY(!pg_conn)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	/* The connection will be closed in da_postgres_connection_free. */
	connection->u.postgres.pg_conn = pg_conn;
	connection->opened = RT_TRUE;

	if (RT_UNLIKELY(PQstatus(pg_conn) != CONNECTION_OK)) {
		da_postgres_utils_set_with_last_error(connection);
		goto error;
	}

	if (!connection->auto_commit) {
		if (RT_UNLIKELY(!da_postgres_connection_execute(connection, "begin")))
			goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_connection_create_statement(struct da_connection *connection, struct da_statement *statement)
{
	rt_s ret;

	if (RT_UNLIKELY(!connection->opened)) {
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	statement->execute = &da_postgres_statement_execute;
	statement->select = &da_postgres_statement_select;
	statement->execute_prepared = &da_postgres_statement_execute_prepared;
	statement->select_prepared = &da_postgres_statement_select_prepared;
	statement->free = &da_postgres_statement_free;

	statement->connection = connection;

	statement->prepared_sql = RT_NULL;
	statement->prepared = RT_FALSE;

	statement->u.postgres.param_formats = RT_NULL;
	statement->u.postgres.param_lengths = RT_NULL;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_connection_prepare_statement(struct da_connection *connection, struct da_statement *statement, const rt_char8 *sql)
{
	struct rt_uuid uuid;
	rt_un buffer_size;
	rt_s ret;

	if (RT_UNLIKELY(!da_postgres_connection_create_statement(connection, statement)))
		goto error;

	statement->prepared_sql = sql;

	if (RT_UNLIKELY(!rt_uuid_create(&uuid))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	buffer_size = 0;
	if (RT_UNLIKELY(!rt_uuid_append8(&uuid, statement->u.postgres.statement_name, 64, &buffer_size))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	statement->u.postgres.statement_name_size = buffer_size;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_connection_commit(struct da_connection *connection)
{
	rt_s ret;
	
	if (RT_UNLIKELY(!da_postgres_connection_execute(connection, "commit")))
		goto error;

	if (!connection->auto_commit) {
		if (RT_UNLIKELY(!da_postgres_connection_execute(connection, "begin")))
			goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_connection_rollback(struct da_connection *connection)
{
	rt_s ret;
	
	if (RT_UNLIKELY(!da_postgres_connection_execute(connection, "rollback")))
		goto error;

	if (!connection->auto_commit) {
		if (RT_UNLIKELY(!da_postgres_connection_execute(connection, "begin")))
			goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_connection_free(struct da_connection *connection)
{
	PGconn *pg_conn = (PGconn*)connection->u.postgres.pg_conn;

	if (connection->opened) {
		/* PQfinish returns void. */
		PQfinish(pg_conn);
	}

	return RT_OK;
}
