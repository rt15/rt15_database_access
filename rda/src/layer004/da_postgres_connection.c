#include "layer004/da_postgres_connection.h"

#include "layer000/da_postgres_headers.h"
#include "layer001/da_postgres_utils.h"
#include "layer003/da_postgres_statement.h"

rt_s da_postgres_connection_open(struct da_connection *connection)
{
	struct da_data_source *data_source = connection->data_source;
	PGconn *pg_conn = RT_NULL;
	rt_s ret;

	if (RT_UNLIKELY(connection->opened)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.postgres.last_error_is_postgres = RT_FALSE;
		goto error;
	}

	pg_conn = PQconnectdbParams(data_source->u.postgres.keywords, data_source->u.postgres.values, 1);
	if (RT_UNLIKELY(!pg_conn)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.postgres.last_error_is_postgres = RT_FALSE;
		goto error;
	}
	/* The connection will be closed in da_postgres_connection_free. */
	connection->u.postgres.pg_conn = pg_conn;
	connection->opened = RT_TRUE;

	if (RT_UNLIKELY(PQstatus(pg_conn) != CONNECTION_OK)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.postgres.last_error_is_postgres = RT_TRUE;
		goto error;
	}

	if (!connection->auto_commit) {
		if (RT_UNLIKELY(!da_postgres_utils_execute(connection, "begin")))
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
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.postgres.last_error_is_postgres = RT_FALSE;
		goto error;
	}

	statement->connection = connection;

	statement->execute = &da_postgres_statement_execute;
	statement->create_result = &da_postgres_statement_create_result;
	statement->free = &da_postgres_statement_free;

	statement->last_error_message_provider.append = &da_postgres_statement_append_last_error_message;

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
	
	if (RT_UNLIKELY(!da_postgres_utils_execute(connection, "commit")))
		goto error;

	if (!connection->auto_commit) {
		if (RT_UNLIKELY(!da_postgres_utils_execute(connection, "begin")))
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
	
	if (RT_UNLIKELY(!da_postgres_utils_execute(connection, "rollback")))
		goto error;

	if (!connection->auto_commit) {
		if (RT_UNLIKELY(!da_postgres_utils_execute(connection, "begin")))
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

rt_s da_postgres_connection_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	struct da_connection *connection = RT_MEMORY_CONTAINER_OF(last_error_message_provider, struct da_connection, last_error_message_provider);

	return da_postres_utils_append_error_message(connection->u.postgres.last_error_is_postgres, connection, buffer, buffer_capacity, buffer_size);
}
