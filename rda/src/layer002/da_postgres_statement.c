#include "layer002/da_oracle_statement.h"

#include "layer000/da_postgres_headers.h"
#include "layer001/da_postgres_utils.h"

rt_s da_postgres_statement_execute(struct da_statement *statement, const rt_char8 *sql)
{
	struct da_connection *connection = statement->connection;
	PGconn *pg_conn = connection->u.postgres.pg_conn;
	PGresult *pg_result = RT_NULL;
	ExecStatusType status;
	rt_s ret;

	/* Getting ride of a possible previous result. */
	if (statement->u.postgres.pg_result) {
		/* PQClear returns void. */
		PQclear(statement->u.postgres.pg_result);
		statement->u.postgres.pg_result = RT_NULL;
	}

	pg_result = PQexec(pg_conn, sql);

	if (RT_UNLIKELY(!pg_result)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.postgres.last_error_is_postgres = RT_TRUE;
		goto error;
	}

	statement->u.postgres.pg_result = pg_result;

	status = PQresultStatus(pg_result);
	if (RT_UNLIKELY(status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.postgres.last_error_is_postgres = RT_TRUE;
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_statement_get_row_count(struct da_statement *statement, rt_un *row_count)
{
	struct da_connection *connection = statement->connection;
	PGresult *pg_result = (PGresult*)statement->u.postgres.pg_result;
	const rt_char8* affected_rows;
	rt_s ret;

	if (RT_UNLIKELY(!pg_result)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.postgres.last_error_is_postgres = RT_FALSE;
		goto error;
	}

	affected_rows = PQcmdTuples(pg_result);
	if (RT_UNLIKELY(!rt_char8_get_size(affected_rows))) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.postgres.last_error_is_postgres = RT_FALSE;
		goto error;
	}
	if (RT_UNLIKELY(!rt_char8_convert_to_un(affected_rows, row_count))) {
		connection->u.postgres.last_error_is_postgres = RT_FALSE;
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_statement_free(struct da_statement *statement)
{
	if (statement->u.postgres.pg_result) {
		/* PQClear returns void. */
		PQclear(statement->u.postgres.pg_result);
		statement->u.postgres.pg_result = RT_NULL;
	}
	return RT_OK;
}

rt_s da_postgres_statement_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	struct da_statement *statement = RT_MEMORY_CONTAINER_OF(last_error_message_provider, struct da_statement, last_error_message_provider);
	struct da_connection *connection = statement->connection;

	return da_postres_utils_append_error_message(connection->u.postgres.last_error_is_postgres, connection, buffer, buffer_capacity, buffer_size);
}
