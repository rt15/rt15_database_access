#include "layer004/da_postgres_data_source.h"

#include "layer000/da_postgres_headers.h"
#include "layer003/da_postgres_connection.h"

rt_s da_postgres_data_source_open(struct da_data_source *data_source)
{
	const rt_char8 **keywords;
	const rt_char8 **values;
	rt_s ret;

	if (RT_UNLIKELY(data_source->opened)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		goto error;
	}

	keywords = data_source->u.postgres.keywords;
	values = data_source->u.postgres.values;

	keywords[0] = "host";
	values[0] = data_source->u.postgres.host_name;

	keywords[1] = "port";
	values[1] = data_source->u.postgres.port;

	keywords[2] = "dbname";
	values[2] = data_source->u.postgres.dbname;

	keywords[3] = "user";
	values[3] = data_source->user_name;

	keywords[4] = "password";
	values[4] = data_source->password;

	keywords[5] = RT_NULL;
	values[5] = RT_NULL;

	data_source->opened = RT_TRUE;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_data_source_create_connection(struct da_data_source *data_source, struct da_connection *connection, rt_b auto_commit)
{
	rt_s ret;

	if (RT_UNLIKELY(!data_source->opened)) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		goto error;
	}

	connection->open = &da_postgres_connection_open;
	connection->create_statement = &da_postgres_connection_create_statement;
	connection->commit = &da_postgres_connection_commit;
	connection->rollback = &da_postgres_connection_rollback;
	connection->free = &da_postgres_connection_free;

	connection->last_error_message_provider.append = &da_postgres_connection_append_last_error_message;

	connection->data_source = data_source;

	connection->auto_commit = auto_commit;

	connection->opened = RT_FALSE;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_data_source_free(struct da_data_source *data_source)
{
	return RT_OK;
}

rt_s da_postgres_data_source_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	return rt_error_message_append_last(buffer, buffer_capacity, buffer_size);
}
