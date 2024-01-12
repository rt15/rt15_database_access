#include "oracle/layer006/da_oracle_driver.h"

#include "oracle/layer000/da_oracle_headers.h"
#include "oracle/layer001/da_oracle_utils.h"
#include "oracle/layer005/da_oracle_data_source.h"

rt_s da_oracle_driver_create_data_source(struct da_driver *driver, struct da_data_source *data_source, const rt_char8 *host_name, rt_un port, const rt_char8 *database, const rt_char8 *user_name, const rt_char8 *password)
{
	OCIEnv *environment_handle = driver->u.oracle.environment_handle;
	OCIError *error_handle;
	rt_b error_handle_created = RT_FALSE;
	OCIServer *server_handle;
	rt_b server_handle_created = RT_FALSE;
	rt_un connection_string_size;
	sword status;
	rt_s ret;

	data_source->open = &da_oracle_data_source_open;
	data_source->create_connection = &da_oracle_data_source_create_connection;
	data_source->free = &da_oracle_data_source_free;

	data_source->driver = driver;

	data_source->opened = RT_FALSE;

	data_source->u.oracle.user_name_size = rt_char8_get_size(user_name);
	if (RT_UNLIKELY(!rt_char8_copy(user_name, data_source->u.oracle.user_name_size, data_source->u.oracle.user_name, DA_IDENTIFIER_SIZE))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	data_source->u.oracle.password_size = rt_char8_get_size(password);
	if (RT_UNLIKELY(!rt_char8_copy(password, data_source->u.oracle.password_size, data_source->u.oracle.password, DA_IDENTIFIER_SIZE))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	/* Build the connection string. */
	connection_string_size = rt_char8_get_size(host_name);
	if (RT_UNLIKELY(!rt_char8_copy(host_name, connection_string_size, data_source->u.oracle.connection_string, DA_CONNECTION_STRING_SIZE))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	if (RT_UNLIKELY(!rt_char8_append_char(':', data_source->u.oracle.connection_string, DA_CONNECTION_STRING_SIZE, &connection_string_size))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	if (RT_UNLIKELY(!rt_char8_append_un(port, 10, data_source->u.oracle.connection_string, DA_CONNECTION_STRING_SIZE, &connection_string_size))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	if (RT_UNLIKELY(!rt_char8_append(database, rt_char8_get_size(database), data_source->u.oracle.connection_string, DA_CONNECTION_STRING_SIZE, &connection_string_size))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	data_source->u.oracle.connection_string_size = connection_string_size;

	/* Error handle. */
	status = OCIHandleAlloc(environment_handle, (void**)&error_handle, OCI_HTYPE_ERROR, 0, NULL);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		da_oracle_utils_set_with_last_error(status, environment_handle, OCI_HTYPE_ENV);
		goto error;
	}
	error_handle_created = RT_TRUE;

	/* Server handle. */
	status = OCIHandleAlloc(environment_handle, (void**)&server_handle, OCI_HTYPE_SERVER, 0, NULL);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		da_oracle_utils_set_with_last_error(status, environment_handle, OCI_HTYPE_ENV);
		goto error;
	}
	server_handle_created = RT_TRUE;

	data_source->u.oracle.error_handle = error_handle;
	data_source->u.oracle.server_handle = server_handle;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;

	if (server_handle_created) {
		OCIHandleFree(server_handle, OCI_HTYPE_SERVER);
	}
	if (error_handle_created) {
		OCIHandleFree(error_handle, OCI_HTYPE_ERROR);
	}

	goto free;
}

rt_s da_oracle_driver_free(struct da_driver *driver)
{
	sword status;
	rt_s ret;

	status = OCIHandleFree((OCIEnv*)driver->u.oracle.environment_handle, OCI_HTYPE_ENV);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		da_oracle_utils_set_with_last_error(status, RT_NULL, 0);
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_oracle_driver_create(struct da_driver *driver)
{
	sword status;
	rt_s ret;

	driver->create_data_source = &da_oracle_driver_create_data_source;
	driver->free = &da_oracle_driver_free;

	/* Initialize environment and get an environment handle. */
	status = OCIEnvCreate((OCIEnv**)&driver->u.oracle.environment_handle, OCI_DEFAULT, NULL, NULL, NULL, NULL, 0, NULL);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		da_oracle_utils_set_with_last_error(status, RT_NULL, 0);
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}
