#include "postgres/layer001/da_postgres_utils.h"

#include "postgres/layer000/da_postgres_headers.h"

rt_s da_postgres_utils_append_error_message(struct da_connection *connection, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	PGconn *pg_conn = connection->u.postgres.pg_conn;
	rt_char8 *error_message;
	rt_char *output;
	rt_un output_size;
	rt_s ret;

	if (connection->u.postgres.last_error_is_postgres) {
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
