#include "mssql/layer002/da_mssql_result.h"

#include "mssql/layer000/da_mssql_headers.h"
#include "mssql/layer001/da_mssql_utils.h"

rt_s da_mssql_result_bind(struct da_result *result, struct da_binding *bindings, rt_un bindings_size)
{
	struct da_statement *statement = result->statement;
	SQLHSTMT statement_handle = statement->u.mssql.statement_handle;
	rt_un column_index;
	SQLSMALLINT c_type;
	void *buffer;
	SQLLEN buffer_capacity;
	SQLRETURN status;
	rt_s ret;

	result->bindings = bindings;
	result->bindings_size = bindings_size;

	for (column_index = 0; column_index < bindings_size; column_index++) {

		switch (bindings[column_index].type) {
		case DA_BINDING_TYPE_CHAR8:
			c_type = SQL_C_CHAR;
			buffer = bindings[column_index].u.char8.buffer;
			buffer_capacity = bindings[column_index].u.char8.buffer_capacity;
			break;
		case DA_BINDING_TYPE_N32:
			c_type = SQL_C_LONG;
			buffer = &bindings[column_index].u.n32;
			buffer_capacity = 0;
			break;
		default:
			rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
			rt_last_error_message_set_with_last_error();
			goto error;
		}
		status = SQLBindCol(statement_handle, column_index + 1, c_type, buffer, buffer_capacity, (rt_n64*)&bindings[column_index].reserved);
		if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
			da_mssql_utils_set_with_last_error(SQL_HANDLE_STMT, statement_handle);
			goto error;
		}
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_mssql_result_fetch(struct da_result *result, rt_b *end_of_rows)
{
	struct da_statement *statement = result->statement;
	SQLHSTMT statement_handle = statement->u.mssql.statement_handle;
	SQLRETURN status;
	rt_un column_index;
	struct da_binding *bindings = result->bindings;
	rt_s ret;

	status = SQLFetch(statement_handle);
	if (status == SQL_NO_DATA) {
		*end_of_rows = RT_TRUE;
	} else {
		if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
			da_mssql_utils_set_with_last_error(SQL_HANDLE_STMT, statement_handle);
			goto error;
		}

		*end_of_rows = RT_FALSE;

		for (column_index = 0; column_index < result->bindings_size; column_index++) {
			if ((rt_n64)bindings[column_index].reserved == SQL_NULL_DATA) {
				bindings[column_index].is_null = RT_TRUE;
			} else {
				bindings[column_index].is_null = RT_FALSE;
				switch (bindings[column_index].type) {
				case DA_BINDING_TYPE_CHAR8:
					bindings[column_index].u.char8.buffer_size = bindings[column_index].reserved;
					break;
				case DA_BINDING_TYPE_N32:
					break;
				default:
					rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
					rt_last_error_message_set_with_last_error();
					goto error;
				}
			}
		}
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_mssql_result_free(RT_UNUSED struct da_result *result)
{
	return RT_OK;
}
