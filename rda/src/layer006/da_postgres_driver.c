#include "layer006/da_postgres_driver.h"

#include "layer000/da_postgres_headers.h"
#include "layer005/da_postgres_data_source.h"

rt_s da_postgres_driver_create_data_source(struct da_driver *driver, struct da_data_source *data_source, const rt_char8 *host_name, rt_un port, const rt_char8 *database, const rt_char8 *user_name, const rt_char8 *password)
{
	rt_un buffer_size;
	rt_s ret;

	data_source->open = &da_postgres_data_source_open;
	data_source->create_connection = &da_postgres_data_source_create_connection;
	data_source->free = &da_postgres_data_source_free;

	data_source->last_error_message_provider.append = &da_postgres_data_source_append_last_error_message;

	data_source->driver = driver;

	data_source->opened = RT_FALSE;

	data_source->user_name_size = rt_char8_get_size(user_name);
	if (RT_UNLIKELY(!rt_char8_copy(user_name, data_source->user_name_size, data_source->user_name, DA_IDENTIFIER_SIZE))) goto error;
	data_source->password_size = rt_char8_get_size(password);
	if (RT_UNLIKELY(!rt_char8_copy(password, data_source->password_size, data_source->password, DA_IDENTIFIER_SIZE))) goto error;

	if (RT_UNLIKELY(!rt_char8_copy(host_name, rt_char8_get_size(host_name), data_source->u.postgres.host_name, DA_IDENTIFIER_SIZE))) goto error;

	buffer_size = 0;
	if (RT_UNLIKELY(!rt_char8_append_un(port, 10, data_source->u.postgres.port, DA_IDENTIFIER_SIZE, &buffer_size))) goto error;

	if (RT_UNLIKELY(!rt_char8_copy(database, rt_char8_get_size(database), data_source->u.postgres.dbname, DA_DB_SIZE))) goto error;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_driver_free(RT_UNUSED struct da_driver *driver)
{
#ifdef RT_DEFINE_WINDOWS
	rt_s ret;

	if (RT_UNLIKELY(!rt_socket_cleanup())) {
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
#else
	return RT_OK;
#endif
}

rt_s da_postgres_driver_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	return rt_error_message_append_last(buffer, buffer_capacity, buffer_size);
}

rt_s da_postgres_driver_create(struct da_driver *driver)
{
#ifdef RT_DEFINE_WINDOWS
	rt_s ret;
#endif

	driver->create_data_source = &da_postgres_driver_create_data_source;
	driver->free = &da_postgres_driver_free;

	driver->last_error_message_provider.append = &da_postgres_driver_append_last_error_message;

#ifdef RT_DEFINE_WINDOWS

	/* Optimization. */
	/* Windows stores internally a reference count on WSAStartup calls. */
	/* Each time we ask a Postgres connection, pq calls WSAStartup. */
	/* So if an application close all connections from time to time, opening new connections would be slow. */
	if (RT_UNLIKELY(!rt_socket_initialize())) {
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
#else
	return RT_OK;
#endif
}
