#include "layer006/da_mssql_driver.h"

#include "layer000/da_mssql_headers.h"
#include "layer005/da_mssql_data_source.h"

rt_s da_mssql_driver_create(struct da_driver *driver)
{
	SQLHENV environment_handle = RT_NULL;
	rt_b environment_handle_created = RT_FALSE;
	SQLRETURN status;
	rt_s ret;

	driver->create_data_source = &da_mssql_driver_create_data_source;
	driver->free = &da_mssql_driver_free;

	driver->u.mssql.environment_handle = RT_NULL;

	status = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &environment_handle);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	environment_handle_created = RT_TRUE;

	status = SQLSetEnvAttr(environment_handle, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, -1);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	driver->u.mssql.environment_handle = environment_handle;

	ret = RT_OK;
free:
	return ret;

error:
	if (environment_handle_created) {
		SQLFreeHandle(SQL_HANDLE_ENV, environment_handle);
	}
	ret = RT_FAILED;
	goto free;
}

rt_s da_mssql_driver_create_data_source(struct da_driver *driver, struct da_data_source *data_source, const rt_char8 *host_name, rt_un port, const rt_char8 *database, const rt_char8 *user_name, const rt_char8 *password)
{
	rt_char8 *connection_string = data_source->u.mssql.connection_string;
	rt_un connection_string_size;
	rt_s ret;

	data_source->open = &da_mssql_data_source_open;
	data_source->create_connection = &da_mssql_data_source_create_connection;
	data_source->free = &da_mssql_data_source_free;

	data_source->driver = driver;

	data_source->opened = RT_FALSE;

	connection_string_size = 46;
	if (RT_UNLIKELY(!rt_char8_copy("DRIVER={ODBC Driver 17 for SQL Server};SERVER=", connection_string_size, connection_string, DA_CONNECTION_STRING_SIZE))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	if (RT_UNLIKELY(!rt_char8_append(host_name, rt_char8_get_size(host_name), connection_string, DA_CONNECTION_STRING_SIZE, &connection_string_size))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	if (RT_UNLIKELY(!rt_char8_append_char(',', connection_string, DA_CONNECTION_STRING_SIZE, &connection_string_size))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	if (RT_UNLIKELY(!rt_char8_append_un(port, 10, connection_string, DA_CONNECTION_STRING_SIZE, &connection_string_size))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	if (RT_UNLIKELY(!rt_char8_append(";DATABASE=", 10, connection_string, DA_CONNECTION_STRING_SIZE, &connection_string_size))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	if (RT_UNLIKELY(!rt_char8_append(database, rt_char8_get_size(database), connection_string, DA_CONNECTION_STRING_SIZE, &connection_string_size))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	if (RT_UNLIKELY(!rt_char8_append(";UID=", 5, connection_string, DA_CONNECTION_STRING_SIZE, &connection_string_size))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	if (RT_UNLIKELY(!rt_char8_append(user_name, rt_char8_get_size(user_name), connection_string, DA_CONNECTION_STRING_SIZE, &connection_string_size))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	if (RT_UNLIKELY(!rt_char8_append(";PWD=", 5, connection_string, DA_CONNECTION_STRING_SIZE, &connection_string_size))) { 
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	if (RT_UNLIKELY(!rt_char8_append(password, rt_char8_get_size(password), connection_string, DA_CONNECTION_STRING_SIZE, &connection_string_size))) {
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

rt_s da_mssql_driver_free(struct da_driver *driver)
{
	SQLHENV environment_handle = driver->u.mssql.environment_handle;
	SQLRETURN status;
	rt_s ret;

	status = SQLFreeHandle(SQL_HANDLE_ENV, environment_handle);
	if (RT_UNLIKELY(!SQL_SUCCEEDED(status))) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
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
