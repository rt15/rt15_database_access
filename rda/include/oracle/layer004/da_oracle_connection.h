#ifndef DA_ORACLE_CONNECTION_H
#define DA_ORACLE_CONNECTION_H

#include "common/layer000/da_types.h"

RT_EXPORT rt_s da_oracle_connection_open(struct da_connection *connection);

RT_EXPORT rt_s da_oracle_connection_create_statement(struct da_connection *connection, struct da_statement *statement);

RT_EXPORT rt_s da_oracle_connection_prepare_statement(struct da_connection *connection, struct da_statement *statement, const rt_char8 *sql);

RT_EXPORT rt_s da_oracle_connection_commit(struct da_connection *connection);

RT_EXPORT rt_s da_oracle_connection_rollback(struct da_connection *connection);

RT_EXPORT rt_s da_oracle_connection_free(struct da_connection *connection);

#endif /* DA_ORACLE_CONNECTION_H */
