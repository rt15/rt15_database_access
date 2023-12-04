#include "layer004/da_oracle_data_source.h"

#include "layer000/da_oracle_headers.h"
#include "layer001/da_oracle_utils.h"
#include "layer003/da_oracle_connection.h"

rt_s da_oracle_data_source_open(struct da_data_source *data_source)
{
	OCIServer *server_handle = data_source->u.oracle.server_handle;
	OCIError *error_handle = data_source->u.oracle.error_handle;
	rt_char8 *db_link = data_source->u.oracle.db_link;
	rt_un db_link_size = data_source->u.oracle.db_link_size;
	sword status;
	rt_s ret;

	if (data_source->opened) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		data_source->u.oracle.last_error_status = OCI_ERROR;
		data_source->u.oracle.last_error_handle = RT_NULL;
		data_source->u.oracle.last_error_handle_type = 0;
		goto error;
	}

	/* Attach server handle to host/port/database. */
	status = OCIServerAttach(server_handle, error_handle, (OraText*)db_link, db_link_size, OCI_DEFAULT);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		data_source->u.oracle.last_error_status = status;
		data_source->u.oracle.last_error_handle = error_handle;
		data_source->u.oracle.last_error_handle_type = OCI_HTYPE_ERROR;
		goto error;
	}
	data_source->opened = RT_TRUE;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_oracle_data_source_create_connection(struct da_data_source *data_source, struct da_connection *connection, rt_b auto_commit)
{
	OCIEnv *environment_handle = data_source->driver->u.oracle.environment_handle;
	OCIServer *server_handle = data_source->u.oracle.server_handle;
	OCIError *error_handle;
	rt_b error_handle_created = RT_FALSE;
	OCISvcCtx *service_context_handle;
	rt_b service_context_handle_created = RT_FALSE;
	OCISession *session_handle;
	rt_b session_handle_created = RT_FALSE;
	sword status;
	rt_s ret;

	if (!data_source->opened) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		data_source->u.oracle.last_error_status = OCI_ERROR;
		data_source->u.oracle.last_error_handle = RT_NULL;
		data_source->u.oracle.last_error_handle_type = 0;
		goto error;
	}

	connection->open = &da_oracle_connection_open;
	connection->create_statement = &da_oracle_connection_create_statement;
	connection->commit = &da_oracle_connection_commit;
	connection->rollback = &da_oracle_connection_rollback;
	connection->free = &da_oracle_connection_free;

	connection->last_error_message_provider.append = &da_oracle_connection_append_last_error_message;

	connection->data_source = data_source;

	connection->auto_commit = auto_commit;

	connection->opened = RT_FALSE;

	/* Error handle. */
	status = OCIHandleAlloc(environment_handle, (void**)&error_handle, OCI_HTYPE_ERROR, 0, NULL);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		data_source->u.oracle.last_error_status = status;
		data_source->u.oracle.last_error_handle = environment_handle;
		data_source->u.oracle.last_error_handle_type = OCI_HTYPE_ENV;
		goto error;
	}
	error_handle_created = RT_TRUE;

	/* Service context handle. */
	status = OCIHandleAlloc(environment_handle, (void**)&service_context_handle, OCI_HTYPE_SVCCTX, 0, NULL);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		data_source->u.oracle.last_error_status = status;
		data_source->u.oracle.last_error_handle = environment_handle;
		data_source->u.oracle.last_error_handle_type = OCI_HTYPE_ENV;
		goto error;
	}
	service_context_handle_created = RT_TRUE;

	/* Put the server in the service context. */
	status = OCIAttrSet(service_context_handle, OCI_HTYPE_SVCCTX, server_handle, 0, OCI_ATTR_SERVER, data_source->u.oracle.error_handle);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		data_source->u.oracle.last_error_status = status;
		data_source->u.oracle.last_error_handle = data_source->u.oracle.error_handle;
		data_source->u.oracle.last_error_handle_type = OCI_HTYPE_ERROR;
		goto error;
	}

	/* Session handle. */
	status = OCIHandleAlloc(environment_handle, (void**)&session_handle, OCI_HTYPE_SESSION, 0, NULL);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		data_source->u.oracle.last_error_status = status;
		data_source->u.oracle.last_error_handle = environment_handle;
		data_source->u.oracle.last_error_handle_type = OCI_HTYPE_ENV;
		goto error;
	}
	session_handle_created = RT_TRUE;

	/* Set user_name in session */
	status = OCIAttrSet(session_handle, OCI_HTYPE_SESSION, data_source->user_name, data_source->user_name_size, OCI_ATTR_USERNAME, data_source->u.oracle.error_handle);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		data_source->u.oracle.last_error_status = status;
		data_source->u.oracle.last_error_handle = data_source->u.oracle.error_handle;
		data_source->u.oracle.last_error_handle_type = OCI_HTYPE_ERROR;
		goto error;
	}

	/* Set password in session */
	status = OCIAttrSet(session_handle, OCI_HTYPE_SESSION, data_source->password, data_source->password_size, OCI_ATTR_PASSWORD, data_source->u.oracle.error_handle);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		data_source->u.oracle.last_error_status = status;
		data_source->u.oracle.last_error_handle = data_source->u.oracle.error_handle;
		data_source->u.oracle.last_error_handle_type = OCI_HTYPE_ERROR;
		goto error;
	}

	connection->u.oracle.error_handle = error_handle;
	connection->u.oracle.service_context_handle = service_context_handle;
	connection->u.oracle.session_handle = session_handle;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;

	if (session_handle_created) {
		OCIHandleFree(session_handle, OCI_HTYPE_SESSION);
	}
	if (service_context_handle_created) {
		OCIHandleFree(service_context_handle, OCI_HTYPE_SVCCTX);
	}
	if (error_handle_created) {
		OCIHandleFree(error_handle, OCI_HTYPE_ERROR);
	}

	goto free;
}

rt_s da_oracle_data_source_free(struct da_data_source *data_source)
{
	sword status;
	rt_s ret = RT_OK;

	if (data_source->opened) {
		status = OCIServerDetach(data_source->u.oracle.server_handle, data_source->u.oracle.error_handle, OCI_DEFAULT);
		if (RT_UNLIKELY(status != OCI_SUCCESS && ret)) {
			rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
			data_source->u.oracle.last_error_status = status;
			data_source->u.oracle.last_error_handle = RT_NULL;
			data_source->u.oracle.last_error_handle_type = 0;
			ret = RT_FAILED;
		}
	}

	status = OCIHandleFree(data_source->u.oracle.server_handle, OCI_HTYPE_SERVER);
	if (RT_UNLIKELY(status != OCI_SUCCESS && ret)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		data_source->u.oracle.last_error_status = status;
		data_source->u.oracle.last_error_handle = RT_NULL;
		data_source->u.oracle.last_error_handle_type = 0;
		ret = RT_FAILED;
	}

	status = OCIHandleFree(data_source->u.oracle.error_handle, OCI_HTYPE_ERROR);
	if (RT_UNLIKELY(status != OCI_SUCCESS && ret)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		data_source->u.oracle.last_error_status = status;
		data_source->u.oracle.last_error_handle = RT_NULL;
		data_source->u.oracle.last_error_handle_type = 0;
		ret = RT_FAILED;
	}

	return ret;
}

rt_s da_oracle_data_source_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	struct da_data_source *data_source = RT_MEMORY_CONTAINER_OF(last_error_message_provider, struct da_data_source, last_error_message_provider);

	return da_oracle_utils_append_error_message(data_source->u.oracle.last_error_status, data_source->u.oracle.last_error_handle, data_source->u.oracle.last_error_handle_type, buffer, buffer_capacity, buffer_size);
}
