#ifndef DA_POSTGRES_CONNECTION_H
#define DA_POSTGRES_CONNECTION_H

#include "rda.h"

RT_EXPORT rt_s da_postgres_connection_open(struct da_connection *connection);

RT_EXPORT rt_s da_postgres_connection_create_statement(struct da_connection *connection, struct da_statement *statement);

RT_EXPORT rt_s da_postgres_connection_prepare_statement(struct da_connection *connection, struct da_statement *statement, const rt_char8 *sql);

RT_EXPORT rt_s da_postgres_connection_commit(struct da_connection *connection);

RT_EXPORT rt_s da_postgres_connection_rollback(struct da_connection *connection);

RT_EXPORT rt_s da_postgres_connection_free(struct da_connection *connection);

#endif /* DA_POSTGRES_CONNECTION_H */
