#include "layer001/da_postgres_utils.h"

#include "layer000/da_postgres_headers.h"

RT_EXPORT rt_s da_postgres_utils_set_with_last_error(struct da_connection *connection)
{
	PGconn *pg_conn = connection->u.postgres.pg_conn;
	rt_char8 *error_message;
	rt_un error_message_size;
	rt_char buffer[RT_CHAR_BIG_STRING_SIZE];
	rt_char *output;
	rt_un output_size;
	rt_s ret;

	error_message = PQerrorMessage(pg_conn);
	error_message_size = rt_char8_get_size(error_message);

	if (RT_UNLIKELY(!rt_encoding_decode(error_message, error_message_size, RT_ENCODING_SYSTEM_DEFAULT, buffer, RT_CHAR_BIG_STRING_SIZE, RT_NULL, RT_NULL, &output, &output_size, RT_NULL))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
	if (RT_UNLIKELY(!rt_last_error_message_set(buffer))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}
