#ifndef DA_MSSQL_CONNECTION_H
#define DA_MSSQL_CONNECTION_H

#include "common/layer000/da_types.h"

RT_EXPORT rt_s da_mssql_connection_open(struct da_connection *connection);

RT_EXPORT rt_s da_mssql_connection_create_statement(struct da_connection *connection, struct da_statement *statement);

RT_EXPORT rt_s da_mssql_connection_prepare_statement(struct da_connection *connection, struct da_statement *statement, const rt_char8 *sql);

RT_EXPORT rt_s da_mssql_connection_commit(struct da_connection *connection);

RT_EXPORT rt_s da_mssql_connection_rollback(struct da_connection *connection);

RT_EXPORT rt_s da_mssql_connection_free(struct da_connection *connection);

RT_EXPORT rt_s da_mssql_connection_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size);

#endif /* DA_MSSQL_CONNECTION_H */
