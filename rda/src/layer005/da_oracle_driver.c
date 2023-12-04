#include "layer005/da_oracle_driver.h"

#include "layer000/da_oracle_headers.h"
#include "layer001/da_oracle_utils.h"
#include "layer004/da_oracle_data_source.h"

rt_s da_oracle_driver_create_data_source(struct da_driver *driver, struct da_data_source *data_source, const rt_char8 *host_name, rt_un port, const rt_char8 *database, const rt_char8 *user_name, const rt_char8 *password)
{
	OCIEnv *environment_handle = driver->u.oracle.environment_handle;
	OCIError *error_handle;
	rt_b error_handle_created = RT_FALSE;
	OCIServer *server_handle;
	rt_b server_handle_created = RT_FALSE;
	rt_un db_link_size;
	sword status;
	rt_s ret;

	data_source->open = &da_oracle_data_source_open;
	data_source->create_connection = &da_oracle_data_source_create_connection;
	data_source->free = &da_oracle_data_source_free;

	data_source->last_error_message_provider.append = &da_oracle_data_source_append_last_error_message;

	data_source->driver = driver;

	data_source->opened = RT_FALSE;

	/* Make sure last error is ready in case an rt_char8 function fails. */
	data_source->u.oracle.last_error_status = OCI_ERROR;
	data_source->u.oracle.last_error_handle = RT_NULL;
	data_source->u.oracle.last_error_handle_type = 0;

	data_source->user_name_size = rt_char8_get_size(user_name);
	if (!rt_char8_copy(user_name, data_source->user_name_size, data_source->user_name, DA_IDENTIFIER_SIZE)) goto error;
	data_source->password_size = rt_char8_get_size(password);
	if (!rt_char8_copy(password, data_source->password_size, data_source->password, DA_IDENTIFIER_SIZE)) goto error;

	/* Build the connection string. */
	db_link_size = rt_char8_get_size(host_name);
	if (RT_UNLIKELY(!rt_char8_copy(host_name, db_link_size, data_source->u.oracle.db_link, DA_DB_LINK_SIZE))) goto error;
	if (RT_UNLIKELY(!rt_char8_append_char(':', data_source->u.oracle.db_link, DA_DB_LINK_SIZE, &db_link_size))) goto error;
	if (RT_UNLIKELY(!rt_char8_append_un(port, 10, data_source->u.oracle.db_link, DA_DB_LINK_SIZE, &db_link_size))) goto error;
	if (RT_UNLIKELY(!rt_char8_append(database, rt_char8_get_size(database), data_source->u.oracle.db_link, DA_DB_LINK_SIZE, &db_link_size))) goto error;
	data_source->u.oracle.db_link_size = db_link_size;

	/* Error handle. */
	status = OCIHandleAlloc(environment_handle, (void**)&error_handle, OCI_HTYPE_ERROR, 0, NULL);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		driver->u.oracle.last_error_status = status;
		driver->u.oracle.last_error_handle = environment_handle;
		driver->u.oracle.last_error_handle_type = OCI_HTYPE_ENV;
		goto error;
	}
	error_handle_created = RT_TRUE;

	/* Server handle. */
	status = OCIHandleAlloc(environment_handle, (void**)&server_handle, OCI_HTYPE_SERVER, 0, NULL);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		driver->u.oracle.last_error_status = status;
		driver->u.oracle.last_error_handle = environment_handle;
		driver->u.oracle.last_error_handle_type = OCI_HTYPE_ENV;
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
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		driver->u.oracle.last_error_status = status;
		driver->u.oracle.last_error_handle = RT_NULL;
		driver->u.oracle.last_error_handle_type = 0;
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_oracle_driver_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	struct da_driver *driver = RT_MEMORY_CONTAINER_OF(last_error_message_provider, struct da_driver, last_error_message_provider);

	return da_oracle_utils_append_error_message(driver->u.oracle.last_error_status, driver->u.oracle.last_error_handle, driver->u.oracle.last_error_handle_type, buffer, buffer_capacity, buffer_size);
}

rt_s da_oracle_driver_create(struct da_driver *driver)
{
	sword status;
	rt_s ret;

	driver->create_data_source = &da_oracle_driver_create_data_source;
	driver->free = &da_oracle_driver_free;

	driver->last_error_message_provider.append = &da_oracle_driver_append_last_error_message;

	/* Initialize environment and get an environment handle. */
	status = OCIEnvCreate((OCIEnv**)&driver->u.oracle.environment_handle, OCI_DEFAULT, NULL, NULL, NULL, NULL, 0, NULL);
	if (RT_UNLIKELY(status != OCI_SUCCESS)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		driver->u.oracle.last_error_status = status;
		driver->u.oracle.last_error_handle = RT_NULL;
		driver->u.oracle.last_error_handle_type = 0;
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}
