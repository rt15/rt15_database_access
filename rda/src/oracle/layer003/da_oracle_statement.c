#include "oracle/layer003/da_oracle_statement.h"

#include "oracle/layer000/da_oracle_headers.h"
#include "oracle/layer001/da_oracle_utils.h"
#include "oracle/layer002/da_oracle_result.h"


struct da_oracle_statement_binding {
	rt_n16 *indicators;
	rt_un16 *lengths;
	rt_char8 *values;
};

static rt_s da_oracle_statement_execute_internal(OCIStmt *statement_handle, OCIError *error_handle, struct da_connection *connection, const rt_char8 *sql, ub4 iters)
{
	OCISvcCtx *service_context_handle = connection->u.oracle.service_context_handle;
	sword status;
	ub4 mode;
	rt_s ret;

	status = OCIStmtPrepare(statement_handle, error_handle, (OraText*)sql, rt_char8_get_size(sql), OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.oracle.last_error_is_oracle = RT_TRUE;
		connection->u.oracle.last_error_status = status;
		connection->u.oracle.last_error_handle = error_handle;
		connection->u.oracle.last_error_handle_type = OCI_HTYPE_ERROR;
		goto error;
	}

	if (connection->auto_commit)
		mode = OCI_COMMIT_ON_SUCCESS;
	else
		mode = OCI_DEFAULT;

	status = OCIStmtExecute(service_context_handle, statement_handle, error_handle, iters, 0, NULL, NULL, mode);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.oracle.last_error_is_oracle = RT_TRUE;
		connection->u.oracle.last_error_status = status;
		connection->u.oracle.last_error_handle = error_handle;
		connection->u.oracle.last_error_handle_type = OCI_HTYPE_ERROR;
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s da_oracle_statement_get_row_count(OCIStmt *statement_handle, OCIError *error_handle, struct da_connection *connection, rt_un *row_count)
{
	ub4 oracle_row_count;
	sword status;
	rt_s ret;

	status = OCIAttrGet(statement_handle, OCI_HTYPE_STMT, &oracle_row_count, 0, OCI_ATTR_ROW_COUNT, error_handle);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.oracle.last_error_is_oracle = RT_TRUE;
		connection->u.oracle.last_error_status = status;
		connection->u.oracle.last_error_handle = error_handle;
		connection->u.oracle.last_error_handle_type = OCI_HTYPE_ERROR;
		goto error;
	}

	*row_count = oracle_row_count;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_oracle_statement_execute(struct da_statement *statement, const rt_char8 *sql, rt_un *row_count)
{
	OCIStmt *statement_handle = statement->u.oracle.statement_handle;
	struct da_connection *connection = statement->connection;
	OCIError *error_handle = connection->u.oracle.error_handle;
	rt_s ret;

	if (RT_UNLIKELY(!da_oracle_statement_execute_internal(statement_handle, error_handle, connection, sql, 1)))
		goto error;

	if (row_count) {
		if (RT_UNLIKELY(!da_oracle_statement_get_row_count(statement_handle, error_handle, connection, row_count)))
			goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

RT_EXPORT rt_s da_oracle_statement_select(struct da_statement *statement, struct da_result *result, const rt_char8 *sql)
{
	OCIStmt *statement_handle = statement->u.oracle.statement_handle;
	struct da_connection *connection = statement->connection;
	OCIError *error_handle = connection->u.oracle.error_handle;
	rt_s ret;

	result->bind = &da_oracle_result_bind;
	result->fetch = &da_oracle_result_fetch;
	result->free = &da_oracle_result_free;

	result->last_error_message_provider.append = &da_oracle_result_append_last_error_message;

	result->statement = statement;

	if (!RT_UNLIKELY(da_oracle_statement_execute_internal(statement_handle, error_handle, connection, sql, 0)))
		goto error;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s da_oracle_statement_execute_prepared_internal(struct da_statement *statement, enum da_binding_type *binding_types, rt_un binding_types_size, void ***batches, rt_un batches_size, ub4 iters)
{
	OCIStmt *statement_handle = statement->u.oracle.statement_handle;
	struct da_connection *connection = statement->connection;
	OCIError *error_handle = connection->u.oracle.error_handle;
	OCISvcCtx *service_context_handle = connection->u.oracle.service_context_handle;
	const rt_char8 *sql = statement->prepared_sql;
	sword status;
	struct da_oracle_statement_binding *bindings = RT_NULL;
	rt_un column_index;
	rt_un row_index;
	rt_char8 parameter_name[8];
	rt_un parameter_name_size;
	rt_b nulls_only;
	rt_n32 *n32_values;
	rt_un max_length;
	rt_un length;
	rt_un16 data_type;
	OCIBind *bind_handle;
	ub4 mode;
	rt_s ret;

	status = OCIStmtPrepare(statement_handle, error_handle, (OraText*)sql, rt_char8_get_size(sql), OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.oracle.last_error_is_oracle = RT_TRUE;
		connection->u.oracle.last_error_status = status;
		connection->u.oracle.last_error_handle = error_handle;
		connection->u.oracle.last_error_handle_type = OCI_HTYPE_ERROR;
		goto error;
	}

	if (RT_UNLIKELY(!rt_static_heap_alloc((void**)&bindings, binding_types_size * sizeof(struct da_oracle_statement_binding)))) {
		connection->u.oracle.last_error_is_oracle = RT_FALSE;
		goto error;
	}
	for (column_index = 0; column_index < binding_types_size; column_index++) {
		bindings[column_index].indicators = RT_NULL;
		bindings[column_index].lengths = RT_NULL;
		bindings[column_index].values = RT_NULL;
	}

	for (column_index = 0; column_index < binding_types_size; column_index++) {

		/* Prepare the parameter name like ":1". */
		parameter_name[0] = ':';
		parameter_name[1] = 0;
		parameter_name_size = 1;
		if (RT_UNLIKELY(!rt_char8_append_un(column_index + 1, 10, parameter_name, 8, &parameter_name_size))) {
			connection->u.oracle.last_error_is_oracle = RT_FALSE;
			goto error;
		}

		if (RT_UNLIKELY(!rt_static_heap_alloc((void**)&bindings[column_index].indicators, batches_size * sizeof(rt_n16)))) {
			connection->u.oracle.last_error_is_oracle = RT_FALSE;
			goto error;
		}

		/* Deal vith nulls. */
		nulls_only = RT_TRUE;
		for (row_index = 0; row_index < batches_size; row_index++) {
			if (batches[row_index][column_index]) {
				bindings[column_index].indicators[row_index] = 0;
				nulls_only = RT_FALSE;
			} else {
				bindings[column_index].indicators[row_index] = -1;
			}
		}

		if (nulls_only) {
			max_length = 0;
		} else {

			if (RT_UNLIKELY(!rt_static_heap_alloc((void**)&bindings[column_index].lengths, batches_size * sizeof(rt_un16)))) {
				connection->u.oracle.last_error_is_oracle = RT_FALSE;
				goto error;
			}

			switch (binding_types[column_index]) {
				case DA_BINDING_TYPE_CHAR8:
					/* Compute lengths and max_length. */
					max_length = 0;
					for (row_index = 0; row_index < batches_size; row_index++) {
						if (batches[row_index][column_index]) {
							length = rt_char8_get_size(batches[row_index][column_index]);
							length++;
							bindings[column_index].lengths[row_index] = length;
							if (length > max_length)
								max_length = length;
						}
					}

					if (RT_UNLIKELY(!rt_static_heap_alloc((void**)&bindings[column_index].values, batches_size * max_length))) {
						connection->u.oracle.last_error_is_oracle = RT_FALSE;
						goto error;
					}

					/* Prepare values array. */
					for (row_index = 0; row_index < batches_size; row_index++) {
						if (batches[row_index][column_index]) {
							if (RT_UNLIKELY(!rt_char8_copy(batches[row_index][column_index], bindings[column_index].lengths[row_index] - 1, &bindings[column_index].values[row_index * max_length], max_length))) {
								connection->u.oracle.last_error_is_oracle = RT_FALSE;
								goto error;
							}
						}
					}

					data_type = SQLT_STR;
					break;
				case DA_BINDING_TYPE_N32:
					if (RT_UNLIKELY(!rt_static_heap_alloc((void**)&bindings[column_index].values, batches_size * sizeof(rt_n32)))) {
						connection->u.oracle.last_error_is_oracle = RT_FALSE;
						goto error;
					}
					n32_values = (rt_n32*)bindings[column_index].values;
					for (row_index = 0; row_index < batches_size; row_index++) {
						if (batches[row_index][column_index])
							n32_values[row_index] = *(rt_n32*)batches[row_index][column_index];
						bindings[column_index].lengths[row_index] = sizeof(int);
					}
					max_length = sizeof(int);
					data_type = SQLT_INT;
					break;
				default:
					rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
					connection->u.oracle.last_error_is_oracle = RT_FALSE;
					goto error;
			}
		}

		bind_handle = RT_NULL;
		status = OCIBindByName(statement_handle, &bind_handle, error_handle, (OraText*)parameter_name, parameter_name_size, bindings[column_index].values, max_length, data_type, bindings[column_index].indicators, bindings[column_index].lengths, RT_NULL, 0, RT_NULL, OCI_DEFAULT);
		if (RT_UNLIKELY(status != OCI_SUCCESS)) {
			rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
			connection->u.oracle.last_error_is_oracle = RT_TRUE;
			connection->u.oracle.last_error_status = status;
			connection->u.oracle.last_error_handle = error_handle;
			connection->u.oracle.last_error_handle_type = OCI_HTYPE_ERROR;
			goto error;
		}
	}

	if (connection->auto_commit)
		mode = OCI_COMMIT_ON_SUCCESS;
	else
		mode = OCI_DEFAULT;

	status = OCIStmtExecute(service_context_handle, statement_handle, error_handle, iters, 0, RT_NULL, RT_NULL, mode);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.oracle.last_error_is_oracle = RT_TRUE;
		connection->u.oracle.last_error_status = status;
		connection->u.oracle.last_error_handle = error_handle;
		connection->u.oracle.last_error_handle_type = OCI_HTYPE_ERROR;
		goto error;
	}

	ret = RT_OK;
free:
	if (bindings) {
		for (column_index = 0; column_index < binding_types_size; column_index++) {
			if (RT_UNLIKELY(!rt_static_heap_free((void**)&bindings[column_index].indicators) && ret)) {
				connection->u.oracle.last_error_is_oracle = RT_FALSE;
				goto error;
			}
			if (RT_UNLIKELY(!rt_static_heap_free((void**)&bindings[column_index].lengths) && ret)) {
				connection->u.oracle.last_error_is_oracle = RT_FALSE;
				goto error;
			}
			if (RT_UNLIKELY(!rt_static_heap_free((void**)&bindings[column_index].values) && ret)) {
				connection->u.oracle.last_error_is_oracle = RT_FALSE;
				goto error;
			}
		}
		if (RT_UNLIKELY(!rt_static_heap_free((void**)&bindings) && ret)) {
			connection->u.oracle.last_error_is_oracle = RT_FALSE;
			goto error;
		}
	}
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_oracle_statement_execute_prepared(struct da_statement *statement, enum da_binding_type *binding_types, rt_un binding_types_size, void ***batches, rt_un batches_size, rt_un *row_count)
{
	OCIStmt *statement_handle = statement->u.oracle.statement_handle;
	struct da_connection *connection = statement->connection;
	OCIError *error_handle = connection->u.oracle.error_handle;
	rt_s ret;

	if (RT_UNLIKELY(!da_oracle_statement_execute_prepared_internal(statement, binding_types, binding_types_size, batches, batches_size, batches_size)))
		goto error;

	if (row_count) {
		if (RT_UNLIKELY(!da_oracle_statement_get_row_count(statement_handle, error_handle, connection, row_count)))
			goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_oracle_statement_select_prepared(struct da_statement *statement, struct da_result *result, enum da_binding_type *binding_types, rt_un binding_types_size, void **bindings)
{
	rt_s ret;

	result->bind = &da_oracle_result_bind;
	result->fetch = &da_oracle_result_fetch;
	result->free = &da_oracle_result_free;

	result->last_error_message_provider.append = &da_oracle_result_append_last_error_message;

	result->statement = statement;

	if (RT_UNLIKELY(!da_oracle_statement_execute_prepared_internal(statement, binding_types, binding_types_size, &bindings, 1, 0)))
		goto error;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_oracle_statement_free(struct da_statement *statement)
{
	struct da_connection *connection = statement->connection;
	sword status;
	rt_s ret = RT_OK;

	status = OCIHandleFree(statement->u.oracle.statement_handle, OCI_HTYPE_STMT);
	if (RT_UNLIKELY(status != OCI_SUCCESS && ret)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.oracle.last_error_is_oracle = RT_TRUE;
		connection->u.oracle.last_error_status = status;
		connection->u.oracle.last_error_handle = RT_NULL;
		connection->u.oracle.last_error_handle_type = 0;
		ret = RT_FAILED;
	}

	return ret;
}

rt_s da_oracle_statement_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	struct da_statement *statement = RT_MEMORY_CONTAINER_OF(last_error_message_provider, struct da_statement, last_error_message_provider);
	struct da_connection *connection = statement->connection;

	return da_oracle_utils_append_error_message(connection->u.oracle.last_error_is_oracle, connection->u.oracle.last_error_status, connection->u.oracle.last_error_handle, connection->u.oracle.last_error_handle_type, buffer, buffer_capacity, buffer_size);
}
