#include "mssql/layer001/da_mssql_utils.h"

#include "mssql/layer000/da_mssql_headers.h"

rt_s da_mssql_utils_append_error_message(struct da_connection *connection, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	SQLSMALLINT handle_type;
	SQLHANDLE handle;
	SQLCHAR sql_state[8];
	SQLINTEGER native_error;
	rt_char8 buffer8[RT_CHAR8_BIG_STRING_SIZE];
	SQLSMALLINT text_length;
	SQLRETURN status;
	rt_char *output;
	rt_un output_size;
	rt_s ret;

	if (connection->u.mssql.last_error_is_mssql) {
		handle_type = connection->u.mssql.last_error_handle_type;
		handle = connection->u.mssql.last_error_handle;
		status = SQLGetDiagRec(handle_type, handle, 1, sql_state, &native_error, (SQLCHAR*)buffer8, RT_CHAR8_BIG_STRING_SIZE, &text_length);
		if (RT_UNLIKELY(!SQL_SUCCEEDED(status)))
			goto error;

		if (RT_UNLIKELY(!rt_encoding_decode(buffer8, text_length, RT_ENCODING_SYSTEM_DEFAULT, &buffer[*buffer_size], buffer_capacity - *buffer_size, RT_NULL, RT_NULL, &output, &output_size, RT_NULL)))
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
