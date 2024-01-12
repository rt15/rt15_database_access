#include "postgres/layer005/da_postgres_data_source.h"

#include "postgres/layer000/da_postgres_headers.h"
#include "postgres/layer004/da_postgres_connection.h"

rt_s da_postgres_data_source_open(struct da_data_source *data_source)
{
	const rt_char8 **keywords;
	const rt_char8 **values;
	rt_s ret;

	if (RT_UNLIKELY(data_source->opened)) {
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		rt_last_error_message_set_with_last_error();
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
	values[3] = data_source->u.postgres.user_name;

	keywords[4] = "password";
	values[4] = data_source->u.postgres.password;

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
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	connection->open = &da_postgres_connection_open;
	connection->create_statement = &da_postgres_connection_create_statement;
	connection->prepare_statement = &da_postgres_connection_prepare_statement;
	connection->commit = &da_postgres_connection_commit;
	connection->rollback = &da_postgres_connection_rollback;
	connection->free = &da_postgres_connection_free;

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

rt_s da_postgres_data_source_free(RT_UNUSED struct da_data_source *data_source)
{
	return RT_OK;
}
