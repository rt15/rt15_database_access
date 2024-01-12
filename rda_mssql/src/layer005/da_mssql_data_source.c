#include "layer005/da_mssql_data_source.h"

#include "layer000/da_mssql_headers.h"
#include "layer004/da_mssql_connection.h"

rt_s da_mssql_data_source_open(struct da_data_source *data_source)
{
	rt_s ret;

	if (RT_UNLIKELY(data_source->opened)) {
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		rt_last_error_message_set_with_last_error();
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

rt_s da_mssql_data_source_create_connection(struct da_data_source *data_source, struct da_connection *connection, rt_b auto_commit)
{
	rt_s ret;

	if (RT_UNLIKELY(!data_source->opened)) {
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	connection->open = &da_mssql_connection_open;
	connection->create_statement = &da_mssql_connection_create_statement;
	connection->prepare_statement = &da_mssql_connection_prepare_statement;
	connection->commit = &da_mssql_connection_commit;
	connection->rollback = &da_mssql_connection_rollback;
	connection->free = &da_mssql_connection_free;

	connection->data_source = data_source;

	connection->auto_commit = auto_commit;

	connection->opened = RT_FALSE;

	connection->u.mssql.connection_handle = RT_NULL;
	connection->u.mssql.connection_handle_created = RT_FALSE;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_mssql_data_source_free(RT_UNUSED struct da_data_source *data_source)
{
	return RT_OK;
}
