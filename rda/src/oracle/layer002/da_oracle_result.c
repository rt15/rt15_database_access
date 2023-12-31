#include "oracle/layer002/da_oracle_result.h"

#include "oracle/layer000/da_oracle_headers.h"
#include "oracle/layer001/da_oracle_utils.h"

rt_s da_oracle_result_bind(struct da_result *result, struct da_binding *bindings, rt_un bindings_size)
{
	struct da_statement *statement = result->statement;
	OCIStmt *statement_handle = statement->u.oracle.statement_handle;
	struct da_connection *connection = statement->connection;
	OCIError *error_handle = connection->u.oracle.error_handle;
	rt_un column_index;
	sword status;
	OCIDefine* define_handle;
	ub2 *rlenp;
	sb2 *indp;
	rt_s ret;

	result->bindings = bindings;
	result->bindings_size = bindings_size;

	for (column_index = 0; column_index < bindings_size; column_index++) {

		define_handle = RT_NULL;
		bindings[column_index].is_null = RT_FALSE;

		switch (bindings[column_index].type) {
		case DA_BINDING_TYPE_CHAR8:

			bindings[column_index].u.char8.buffer_size = 0;
			rlenp = (ub2*)&bindings[column_index].u.char8.buffer_size;
			indp = (sb2*)&bindings[column_index].is_null;

			status = OCIDefineByPos(statement_handle, &define_handle, error_handle, column_index + 1, bindings[column_index].u.char8.buffer, bindings[column_index].u.char8.buffer_capacity, SQLT_STR, indp, rlenp, RT_NULL, OCI_DEFAULT);
			break;
		case DA_BINDING_TYPE_N32:
			rlenp = (ub2*)&bindings[column_index].u.char8.buffer_size;
			indp = (sb2*)&bindings[column_index].is_null;

			status = OCIDefineByPos(statement_handle, &define_handle, error_handle, column_index + 1, &bindings[column_index].u.n32.value, sizeof(rt_n32), SQLT_INT, indp, rlenp, RT_NULL, OCI_DEFAULT);
			break;
		default:
			rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
			connection->u.oracle.last_error_is_oracle = RT_FALSE;
			goto error;
		}

		if (RT_UNLIKELY(status != OCI_SUCCESS)) {
			rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
			connection->u.oracle.last_error_is_oracle = RT_TRUE;
			connection->u.oracle.last_error_status = status;
			connection->u.oracle.last_error_handle = error_handle;
			connection->u.oracle.last_error_handle_type = OCI_HTYPE_ERROR;
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

rt_s da_oracle_result_fetch(struct da_result *result, rt_b *end_of_rows)
{
	struct da_statement *statement = result->statement;
	OCIStmt *statement_handle = statement->u.oracle.statement_handle;
	struct da_connection *connection = statement->connection;
	OCIError *error_handle = connection->u.oracle.error_handle;
	sword status;
	rt_un column_index;
	sb2 indicator;
	rt_s ret;

	status = OCIStmtFetch(statement_handle, error_handle, 1, OCI_FETCH_NEXT, OCI_DEFAULT);
	if (status == OCI_NO_DATA) {
		*end_of_rows = RT_TRUE;
	} else {
		*end_of_rows = RT_FALSE;
		if (RT_UNLIKELY(status != OCI_SUCCESS)) {
			rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
			connection->u.oracle.last_error_is_oracle = RT_TRUE;
			connection->u.oracle.last_error_status = status;
			connection->u.oracle.last_error_handle = error_handle;
			connection->u.oracle.last_error_handle_type = OCI_HTYPE_ERROR;
			goto error;
		}

		/* We stored the indicator value in the is_null field. */
		/* So now we exploit the indicator and set is_null accordingly. */
		for (column_index = 0; column_index < result->bindings_size; column_index++) {
			indicator = (sb2)result->bindings[column_index].is_null;
			switch (indicator) {
			case 0:
				/* All good. */
				result->bindings[column_index].is_null = RT_FALSE;
				break;
			case -1:
				/* Null value. */
				result->bindings[column_index].is_null = RT_TRUE;
				break;
			default:
				/* Provided buffer was too small. */
				rt_error_set_last(RT_ERROR_INSUFFICIENT_BUFFER);
				connection->u.oracle.last_error_is_oracle = RT_FALSE;
				goto error;
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

rt_s da_oracle_result_free(RT_UNUSED struct da_result *result)
{
	return RT_OK;
}

rt_s da_oracle_result_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	struct da_result *result = RT_MEMORY_CONTAINER_OF(last_error_message_provider, struct da_result, last_error_message_provider);
	struct da_statement *statement = result->statement;
	struct da_connection *connection = statement->connection;

	return da_oracle_utils_append_error_message(connection->u.oracle.last_error_is_oracle, connection->u.oracle.last_error_status, connection->u.oracle.last_error_handle, connection->u.oracle.last_error_handle_type, buffer, buffer_capacity, buffer_size);
}
