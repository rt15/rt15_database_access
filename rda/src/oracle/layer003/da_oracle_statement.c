#include "oracle/layer003/da_oracle_statement.h"

#include "oracle/layer000/da_oracle_headers.h"
#include "oracle/layer001/da_oracle_utils.h"
#include "oracle/layer002/da_oracle_result.h"

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

rt_s da_oracle_statement_execute(struct da_statement *statement, const rt_char8 *sql, rt_un *row_count)
{
	OCIStmt *statement_handle = statement->u.oracle.statement_handle;
	struct da_connection *connection = statement->connection;
	OCIError *error_handle = connection->u.oracle.error_handle;
	sword status;
	ub4 oracle_row_count;
	rt_s ret;

	if (RT_UNLIKELY(!da_oracle_statement_execute_internal(statement_handle, error_handle, connection, sql, 1)))
		goto error;

	if (row_count) {
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
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

RT_EXPORT rt_s da_oracle_statement_create_result(struct da_statement *statement, struct da_result *result, const rt_char8 *sql)
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
