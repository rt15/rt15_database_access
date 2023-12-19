#include "postgres/layer001/da_postgres_utils.h"

#include "postgres/layer000/da_postgres_headers.h"

rt_s da_postgres_utils_execute(struct da_connection *connection, const rt_char8 *sql)
{
	PGconn *pg_conn = connection->u.postgres.pg_conn;
	PGresult *pg_result = RT_NULL;
	rt_s ret;

	pg_result = PQexec(pg_conn, sql);
	if (RT_UNLIKELY(!pg_result)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.postgres.last_error_is_postgres = RT_TRUE;
		goto error;
	}
	if (RT_UNLIKELY(PQresultStatus(pg_result) != PGRES_COMMAND_OK)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.postgres.last_error_is_postgres = RT_TRUE;
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

rt_s da_postres_utils_append_error_message(rt_b is_postgres, struct da_connection *connection, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	PGconn *pg_conn = connection->u.postgres.pg_conn;
	rt_char8 *error_message;
	rt_char *output;
	rt_un output_size;
	rt_s ret;

	if (is_postgres) {
		error_message = PQerrorMessage(pg_conn);
		if (RT_UNLIKELY(!rt_encoding_decode(error_message, rt_char8_get_size(error_message), RT_ENCODING_SYSTEM_DEFAULT, &buffer[*buffer_size], buffer_capacity - *buffer_size, RT_NULL, RT_NULL, &output, &output_size, RT_NULL)))
			goto error;

		*buffer_size += output_size;
	} else {
		if (RT_UNLIKELY(!rt_error_message_append_last(buffer, buffer_capacity, buffer_size)))
			goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}
