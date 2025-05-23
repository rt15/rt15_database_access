#include "layer003/da_mssql_statement.h"

#include "layer000/da_mssql_headers.h"
#include "layer001/da_mssql_utils.h"
#include "layer002/da_mssql_result.h"

struct da_mssql_statement_binding {
	rt_char8 *values;
	SQLLEN *lengths;
};

static rt_s da_mssql_statement_get_row_count(struct da_statement *statement, rt_un *row_count)
{
	SQLHSTMT statement_handle = statement->u.mssql.statement_handle;
	SQLLEN mssql_row_count;
	SQLRETURN status;
	rt_s ret = RT_FAILED;

	status = SQLRowCount(statement_handle, &mssql_row_count);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		da_mssql_utils_set_with_last_error(SQL_HANDLE_STMT, statement_handle);
		goto end;
	}

	if (statement->prepared_sql) {
		/* SQLRowCount returns the total row count if the prepared statement is executed multiple times. */
		*row_count = mssql_row_count - statement->u.mssql.cumulative_row_count;
		statement->u.mssql.cumulative_row_count = mssql_row_count;
	} else {
		*row_count = mssql_row_count;
	}

	ret = RT_OK;
end:
	return ret;
}

rt_s da_mssql_statement_execute(struct da_statement *statement, const rt_char8 *sql, rt_un *row_count)
{
	SQLHSTMT statement_handle = statement->u.mssql.statement_handle;
	SQLRETURN status;
	rt_s ret = RT_FAILED;

	status = SQLExecDirect(statement_handle, (SQLCHAR*)sql, SQL_NTS);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		da_mssql_utils_set_with_last_error(SQL_HANDLE_STMT, statement_handle);
		goto end;
	}

	if (row_count) {
		if (RT_UNLIKELY(!da_mssql_statement_get_row_count(statement, row_count)))
			goto end;
	}

	ret = RT_OK;
end:
	return ret;
}

rt_s da_mssql_statement_select(struct da_statement *statement, struct da_result *result, const rt_char8 *sql)
{
	rt_s ret = RT_FAILED;

	result->bind = &da_mssql_result_bind;
	result->fetch = &da_mssql_result_fetch;
	result->free = &da_mssql_result_free;

	result->statement = statement;

	if (RT_UNLIKELY(!da_mssql_statement_execute(statement, sql, RT_NULL)))
		goto end;

	ret = RT_OK;
end:
	return ret;
}

static rt_s da_mssql_statement_execute_prepared_with_one_batch(struct da_statement *statement, enum da_binding_type *binding_types, rt_un binding_types_size, void **batches, rt_un *row_count)
{
	SQLHSTMT statement_handle = statement->u.mssql.statement_handle;
	SQLRETURN status;
	SQLLEN *lengths = RT_NULL;
	rt_un column_index;
	SQLSMALLINT c_type;
	SQLSMALLINT sql_type;
	rt_s ret = RT_FAILED;

	if (!statement->prepared) {
		status = SQLPrepare(statement_handle, (SQLCHAR*)statement->prepared_sql, SQL_NTS);
		if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
			da_mssql_utils_set_with_last_error(SQL_HANDLE_STMT, statement_handle);
			goto end;
		}
		statement->prepared = RT_TRUE;
	}

	status = SQLSetStmtAttr(statement_handle, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)1, SQL_IS_UINTEGER);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		da_mssql_utils_set_with_last_error(SQL_HANDLE_STMT, statement_handle);
		goto end;
	}

	if (RT_UNLIKELY(!rt_static_heap_alloc((void**)&lengths, binding_types_size * sizeof(SQLLEN)))) {
		rt_last_error_message_set_with_last_error();
		goto end;
	}

	for (column_index = 0; column_index < binding_types_size; column_index++) {

		switch (binding_types[column_index]) {
		case DA_BINDING_TYPE_CHAR8:
			c_type = SQL_C_CHAR;
			sql_type = SQL_VARCHAR;
			break;
		case DA_BINDING_TYPE_N32:
			c_type = SQL_C_LONG;
			sql_type = SQL_INTEGER;
			break;
		default:
			rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
			rt_last_error_message_set_with_last_error();
			goto end;
		}

		if (batches[column_index]) {
			switch (binding_types[column_index]) {
			case DA_BINDING_TYPE_CHAR8:
				lengths[column_index] = SQL_NTS;
				break;
			case DA_BINDING_TYPE_N32:
				lengths[column_index] = 0;
				break;
			default:
				rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
				rt_last_error_message_set_with_last_error();
				goto end;
			}
		} else {
			lengths[column_index] = SQL_NULL_DATA;
		}

		status = SQLBindParameter(statement_handle, column_index + 1, SQL_PARAM_INPUT, c_type, sql_type, 0, 0, batches[column_index], 0, &lengths[column_index]);
		if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
			da_mssql_utils_set_with_last_error(SQL_HANDLE_STMT, statement_handle);
			goto end;
		}
	}

	status = SQLExecute(statement_handle);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		da_mssql_utils_set_with_last_error(SQL_HANDLE_STMT, statement_handle);
		goto end;
	}

	if (row_count) {
		if (RT_UNLIKELY(!da_mssql_statement_get_row_count(statement, row_count)))
			goto end;
	}

	ret = RT_OK;
end:
	if (RT_UNLIKELY(!rt_static_heap_free((void**)&lengths))) {
		rt_last_error_message_set_with_last_error();
		ret = RT_FAILED;
	}

	return ret;
}

static rt_s da_mssql_statement_execute_prepared_with_multiple_batches(struct da_statement *statement, enum da_binding_type *binding_types, rt_un binding_types_size, void ***batches, rt_un batches_size, rt_un *row_count)
{
	SQLHSTMT statement_handle = statement->u.mssql.statement_handle;
	SQLRETURN status;
	rt_un column_index;
	rt_un row_index;
	struct da_mssql_statement_binding *bindings = RT_NULL;
	rt_b nulls_only;
	rt_un length;
	rt_un max_length;
	SQLINTEGER *n_values;
	SQLSMALLINT c_type;
	SQLSMALLINT sql_type;
	rt_s ret = RT_FAILED;

	if (!statement->prepared) {
		status = SQLPrepare(statement_handle, (SQLCHAR*)statement->prepared_sql, SQL_NTS);
		if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
			da_mssql_utils_set_with_last_error(SQL_HANDLE_STMT, statement_handle);
			goto end;
		}
		statement->prepared = RT_TRUE;
	}

	status = SQLSetStmtAttr(statement_handle, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)batches_size, SQL_IS_UINTEGER);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		da_mssql_utils_set_with_last_error(SQL_HANDLE_STMT, statement_handle);
		goto end;
	}

	if (RT_UNLIKELY(!rt_static_heap_alloc((void**)&bindings, binding_types_size * sizeof(struct da_mssql_statement_binding)))) {
		rt_last_error_message_set_with_last_error();
		goto end;
	}

	for (column_index = 0; column_index < binding_types_size; column_index++) {
		bindings[column_index].values = RT_NULL;
		bindings[column_index].lengths = RT_NULL;
	}

	for (column_index = 0; column_index < binding_types_size; column_index++) {

		if (RT_UNLIKELY(!rt_static_heap_alloc((void**)&bindings[column_index].lengths, batches_size * sizeof(SQLLEN)))) {
			rt_last_error_message_set_with_last_error();
			goto end;
		}

		nulls_only = RT_TRUE;
		max_length = 0;
		for (row_index = 0; row_index < batches_size; row_index++) {
			if (batches[row_index][column_index]) {
				nulls_only = RT_FALSE;
				switch (binding_types[column_index]) {
				case DA_BINDING_TYPE_CHAR8:
					length = rt_char8_get_size(batches[row_index][column_index]);
					bindings[column_index].lengths[row_index] = length;
					if (length > max_length)
						max_length = length;
					break;
				case DA_BINDING_TYPE_N32:
					bindings[column_index].lengths[row_index] = 0;
					break;
				default:
					rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
					rt_last_error_message_set_with_last_error();
					goto end;
				}
			} else {
				bindings[column_index].lengths[row_index] = SQL_NULL_DATA;
			}
		}
		if (!nulls_only) {
			/* Include the terminating zero in the size. */
			max_length++;

			switch (binding_types[column_index]) {
			case DA_BINDING_TYPE_CHAR8:
				c_type = SQL_C_CHAR;
				sql_type = SQL_VARCHAR;
				if (RT_UNLIKELY(!rt_static_heap_alloc((void**)&bindings[column_index].values, batches_size * max_length))) {
					rt_last_error_message_set_with_last_error();
					goto end;
				}
				break;
			case DA_BINDING_TYPE_N32:
				c_type = SQL_C_LONG;
				sql_type = SQL_INTEGER;
				if (RT_UNLIKELY(!rt_static_heap_alloc((void**)&bindings[column_index].values, batches_size * sizeof(SQLINTEGER)))) {
					rt_last_error_message_set_with_last_error();
					goto end;
				}
				n_values = (SQLINTEGER*)bindings[column_index].values;
				break;
			default:
				rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
				rt_last_error_message_set_with_last_error();
				goto end;
			}
			
			for (row_index = 0; row_index < batches_size; row_index++) {
				if (batches[row_index][column_index]) {
					switch (binding_types[column_index]) {
					case DA_BINDING_TYPE_CHAR8:
						if (RT_UNLIKELY(!rt_char8_copy(batches[row_index][column_index], bindings[column_index].lengths[row_index], &bindings[column_index].values[row_index * max_length], max_length))) {
							rt_last_error_message_set_with_last_error();
							goto end;
						}
						break;
					case DA_BINDING_TYPE_N32:
						n_values[row_index] = *(rt_n32*)batches[row_index][column_index];
						break;
					default:
						rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
						rt_last_error_message_set_with_last_error();
						goto end;
					}
				}
			}
		}

		status = SQLBindParameter(statement_handle, column_index + 1, SQL_PARAM_INPUT, c_type, sql_type, max_length, 0, bindings[column_index].values, max_length, bindings[column_index].lengths);
		if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
			da_mssql_utils_set_with_last_error(SQL_HANDLE_STMT, statement_handle);
			goto end;
		}
	}

	status = SQLExecute(statement_handle);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		da_mssql_utils_set_with_last_error(SQL_HANDLE_STMT, statement_handle);
		goto end;
	}

	if (row_count) {
		if (RT_UNLIKELY(!da_mssql_statement_get_row_count(statement, row_count)))
			goto end;
	}

	ret = RT_OK;
end:

	if (bindings) {
		for (column_index = 0; column_index < binding_types_size; column_index++) {
			if (RT_UNLIKELY(!rt_static_heap_free((void**)&bindings[column_index].lengths))) {
				rt_last_error_message_set_with_last_error();
				ret = RT_FAILED;
			}
			if (RT_UNLIKELY(!rt_static_heap_free((void**)&bindings[column_index].values))) {
				rt_last_error_message_set_with_last_error();
				ret = RT_FAILED;
			}
		}
		if (RT_UNLIKELY(!rt_static_heap_free((void**)&bindings))) {
			rt_last_error_message_set_with_last_error();
			ret = RT_FAILED;
		}
	}

	return ret;
}

rt_s da_mssql_statement_execute_prepared(struct da_statement *statement, enum da_binding_type *binding_types, rt_un binding_types_size, void ***batches, rt_un batches_size, rt_un *row_count)
{
	rt_s ret;

	if (batches_size == 1)
		ret = da_mssql_statement_execute_prepared_with_one_batch(statement, binding_types, binding_types_size, *batches, row_count);
	else
		ret = da_mssql_statement_execute_prepared_with_multiple_batches(statement, binding_types, binding_types_size, batches, batches_size, row_count);

	return ret;
}

rt_s da_mssql_statement_select_prepared(struct da_statement *statement, struct da_result *result, enum da_binding_type *binding_types, rt_un binding_types_size, void **bindings)
{
	rt_s ret = RT_FAILED;

	result->bind = &da_mssql_result_bind;
	result->fetch = &da_mssql_result_fetch;
	result->free = &da_mssql_result_free;

	result->statement = statement;

	if (RT_UNLIKELY(!da_mssql_statement_execute_prepared_with_one_batch(statement, binding_types, binding_types_size, bindings, RT_NULL)))
		goto end;

	ret = RT_OK;
end:
	return ret;
}

rt_s da_mssql_statement_free(struct da_statement *statement)
{
	SQLHSTMT statement_handle = statement->u.mssql.statement_handle;
	SQLRETURN status;
	rt_s ret = RT_FAILED;

	status = SQLFreeHandle(SQL_HANDLE_STMT, statement_handle);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		rt_last_error_message_set_with_last_error();
		goto end;
	}

	ret = RT_OK;
end:
	return ret;
}
