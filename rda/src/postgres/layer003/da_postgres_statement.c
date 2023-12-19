#include "oracle/layer003/da_oracle_statement.h"

#include "postgres/layer000/da_postgres_headers.h"
#include "postgres/layer001/da_postgres_utils.h"
#include "postgres/layer002/da_postgres_result.h"

static rt_s da_postgres_statement_execute_internal(struct da_connection *connection, const rt_char8 *sql, PGresult **pg_result)
{
	PGconn *pg_conn = connection->u.postgres.pg_conn;
	PGresult *local_pg_result = RT_NULL;
	ExecStatusType status;
	rt_s ret;

	local_pg_result = PQexec(pg_conn, sql);

	if (RT_UNLIKELY(!local_pg_result)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.postgres.last_error_is_postgres = RT_TRUE;
		goto error;
	}

	status = PQresultStatus(local_pg_result);
	if (RT_UNLIKELY(status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.postgres.last_error_is_postgres = RT_TRUE;
		goto error;
	}

	*pg_result = local_pg_result;

	ret = RT_OK;
free:
	return ret;

error:
	if (local_pg_result)
		PQclear(local_pg_result);

	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_statement_execute(struct da_statement *statement, const rt_char8 *sql, rt_un *row_count)
{
	struct da_connection *connection = statement->connection;
	PGresult *pg_result;
	rt_b pg_result_created = RT_FALSE;
	const rt_char8* affected_rows;
	rt_s ret;

	if (RT_UNLIKELY(!da_postgres_statement_execute_internal(connection, sql, &pg_result)))
		goto error;
	pg_result_created = RT_TRUE;

	if (row_count) {
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
	}

	ret = RT_OK;
free:
	if (pg_result_created) {
		pg_result_created = RT_FALSE;
		/* PQclear returns void. */
		PQclear(pg_result);
	}
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_statement_create_result(struct da_statement *statement, struct da_result *result, const rt_char8 *sql)
{
	struct da_connection *connection = statement->connection;
	PGresult *pg_result;
	rt_b pg_result_created = RT_FALSE;
	rt_s ret;

	result->bind = &da_postgres_result_bind;
	result->fetch = &da_postgres_result_fetch;
	result->free = &da_postgres_result_free;

	result->last_error_message_provider.append = &da_postgres_result_append_last_error_message;

	result->statement = statement;

	if (RT_UNLIKELY(!da_postgres_statement_execute_internal(connection, sql, &pg_result)))
		goto error;
	pg_result_created = RT_TRUE;

	result->u.postgres.pg_result = pg_result;
	result->u.postgres.row_count = PQntuples(pg_result);
	result->u.postgres.current_row = 0;

	ret = RT_OK;
free:
	return ret;

error:
	if (pg_result_created)
		PQclear(pg_result);

	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_statement_free(RT_UNUSED struct da_statement *statement)
{
	return RT_OK;
}

rt_s da_postgres_statement_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	struct da_statement *statement = RT_MEMORY_CONTAINER_OF(last_error_message_provider, struct da_statement, last_error_message_provider);
	struct da_connection *connection = statement->connection;

	return da_postres_utils_append_error_message(connection->u.postgres.last_error_is_postgres, connection, buffer, buffer_capacity, buffer_size);
}
