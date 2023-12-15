#include "layer004/da_oracle_connection.h"

#include "layer000/da_oracle_headers.h"
#include "layer001/da_oracle_utils.h"
#include "layer003/da_oracle_statement.h"

rt_s da_oracle_connection_open(struct da_connection *connection)
{
	OCISvcCtx *service_context_handle = connection->u.oracle.service_context_handle;
	OCIError *error_handle = connection->u.oracle.error_handle;
	OCISession *session_handle = connection->u.oracle.session_handle;
	sword status;
	rt_s ret;

	if (RT_UNLIKELY(connection->opened)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.oracle.last_error_is_oracle = RT_FALSE;
		goto error;
	}

	/* Open the connection. */
	status = OCISessionBegin(service_context_handle, error_handle, session_handle, OCI_CRED_RDBMS, OCI_DEFAULT);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.oracle.last_error_is_oracle = RT_TRUE;
		connection->u.oracle.last_error_status = status;
		connection->u.oracle.last_error_handle = error_handle;
		connection->u.oracle.last_error_handle_type = OCI_HTYPE_ERROR;
		goto error;
	}
	connection->opened = RT_TRUE;

	/* Associate the session with the service context */
	status = OCIAttrSet(service_context_handle, OCI_HTYPE_SVCCTX, session_handle, 0, OCI_ATTR_SESSION, error_handle);
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

rt_s da_oracle_connection_create_statement(struct da_connection *connection, struct da_statement *statement)
{
	OCIEnv *environment_handle = connection->data_source->driver->u.oracle.environment_handle;
	OCIStmt *statement_handle;
	rt_b statement_handle_created = RT_FALSE;
	sword status;
	rt_s ret;

	if (RT_UNLIKELY(!connection->opened)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.oracle.last_error_is_oracle = RT_FALSE;
		goto error;
	}

	statement->execute = &da_oracle_statement_execute;
	statement->free = &da_oracle_statement_free;

	statement->last_error_message_provider.append = &da_oracle_statement_append_last_error_message;

	statement->connection = connection;

	/* Statement handle. */
	status = OCIHandleAlloc(environment_handle, (void**)&statement_handle, OCI_HTYPE_STMT, 0, NULL);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.oracle.last_error_is_oracle = RT_TRUE;
		connection->u.oracle.last_error_status = status;
		connection->u.oracle.last_error_handle = environment_handle;
		connection->u.oracle.last_error_handle_type = OCI_HTYPE_ENV;
		goto error;
	}
	statement_handle_created = RT_TRUE;

	statement->u.oracle.statement_handle = statement_handle;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;

	if (statement_handle_created) {
		OCIHandleFree(statement_handle, OCI_HTYPE_STMT);
	}

	goto free;
}

rt_s da_oracle_connection_commit(struct da_connection *connection)
{
	OCISvcCtx *service_context_handle = connection->u.oracle.service_context_handle;
	OCIError *error_handle = connection->u.oracle.error_handle;
	sword status;
	rt_s ret;

	status = OCITransCommit(service_context_handle, error_handle, OCI_DEFAULT);
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

rt_s da_oracle_connection_rollback(struct da_connection *connection)
{
	OCISvcCtx *service_context_handle = connection->u.oracle.service_context_handle;
	OCIError *error_handle = connection->u.oracle.error_handle;
	sword status;
	rt_s ret;

	status = OCITransRollback(service_context_handle, error_handle, OCI_DEFAULT);
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

rt_s da_oracle_connection_free(struct da_connection *connection)
{
	sword status;
	rt_s ret = RT_OK;

	if (connection->opened) {

		/* OCISessionEnd makes an implicit commit so we must rollback before that. */
		if (!connection->auto_commit) {
			status = OCITransRollback(connection->u.oracle.service_context_handle, connection->u.oracle.error_handle, OCI_DEFAULT);
			if (RT_UNLIKELY(status != OCI_SUCCESS && ret)) {
				rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
				connection->u.oracle.last_error_is_oracle = RT_TRUE;
				connection->u.oracle.last_error_status = status;
				connection->u.oracle.last_error_handle = RT_NULL;
				connection->u.oracle.last_error_handle_type = 0;
				ret = RT_FAILED;
			}
		}

		status = OCISessionEnd(connection->u.oracle.service_context_handle, connection->u.oracle.error_handle, connection->u.oracle.session_handle, OCI_DEFAULT);
		if (RT_UNLIKELY(status != OCI_SUCCESS && ret)) {
			rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
			connection->u.oracle.last_error_is_oracle = RT_TRUE;
			connection->u.oracle.last_error_status = status;
			connection->u.oracle.last_error_handle = RT_NULL;
			connection->u.oracle.last_error_handle_type = 0;
			ret = RT_FAILED;
		}
	}

	status = OCIHandleFree(connection->u.oracle.session_handle, OCI_HTYPE_SESSION);
	if (RT_UNLIKELY(status != OCI_SUCCESS && ret)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.oracle.last_error_is_oracle = RT_TRUE;
		connection->u.oracle.last_error_status = status;
		connection->u.oracle.last_error_handle = RT_NULL;
		connection->u.oracle.last_error_handle_type = 0;
		ret = RT_FAILED;
	}

	status = OCIHandleFree(connection->u.oracle.service_context_handle, OCI_HTYPE_SVCCTX);
	if (RT_UNLIKELY(status != OCI_SUCCESS && ret)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		connection->u.oracle.last_error_is_oracle = RT_TRUE;
		connection->u.oracle.last_error_status = status;
		connection->u.oracle.last_error_handle = RT_NULL;
		connection->u.oracle.last_error_handle_type = 0;
		ret = RT_FAILED;
	}

	status = OCIHandleFree(connection->u.oracle.error_handle, OCI_HTYPE_ERROR);
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

rt_s da_oracle_connection_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	struct da_connection *connection = RT_MEMORY_CONTAINER_OF(last_error_message_provider, struct da_connection, last_error_message_provider);

	return da_oracle_utils_append_error_message(connection->u.oracle.last_error_is_oracle, connection->u.oracle.last_error_status, connection->u.oracle.last_error_handle, connection->u.oracle.last_error_handle_type, buffer, buffer_capacity, buffer_size);
}
