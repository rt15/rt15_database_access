#include "layer001/da_mssql_utils.h"

#include "layer000/da_mssql_headers.h"

rt_s da_mssql_utils_set_with_last_error(rt_n16 handle_type, void *handle)
{
	SQLCHAR sql_state[8];
	SQLINTEGER native_error;
	rt_char8 buffer8[RT_CHAR8_HALF_BIG_STRING_SIZE];
	rt_char buffer[RT_CHAR_HALF_BIG_STRING_SIZE];
	SQLSMALLINT text_length;
	SQLRETURN status;
	rt_char *output;
	rt_un output_size;
	rt_s ret;

	status = SQLGetDiagRec(handle_type, handle, 1, sql_state, &native_error, (SQLCHAR*)buffer8, RT_CHAR8_HALF_BIG_STRING_SIZE, &text_length);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	if (RT_UNLIKELY(!rt_encoding_decode(buffer8, text_length, RT_ENCODING_SYSTEM_DEFAULT, buffer, RT_CHAR_HALF_BIG_STRING_SIZE, RT_NULL, RT_NULL, &output, &output_size, RT_NULL))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}

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
