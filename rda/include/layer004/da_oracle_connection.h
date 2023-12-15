#ifndef DA_ORACLE_CONNECTION_H
#define DA_ORACLE_CONNECTION_H

#include "layer000/da_types.h"

RT_EXPORT rt_s da_oracle_connection_open(struct da_connection *connection);

RT_EXPORT rt_s da_oracle_connection_create_statement(struct da_connection *connection, struct da_statement *statement);

RT_EXPORT rt_s da_oracle_connection_commit(struct da_connection *connection);

RT_EXPORT rt_s da_oracle_connection_rollback(struct da_connection *connection);

RT_EXPORT rt_s da_oracle_connection_free(struct da_connection *connection);

RT_EXPORT rt_s da_oracle_connection_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size);

#endif /* DA_ORACLE_CONNECTION_H */
