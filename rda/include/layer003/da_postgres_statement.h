#ifndef DA_POSTGRES_STATEMENT_H
#define DA_POSTGRES_STATEMENT_H

#include "layer000/da_types.h"

RT_EXPORT rt_s da_postgres_statement_execute(struct da_statement *statement, const rt_char8 *sql, rt_un *row_count);

RT_EXPORT rt_s da_postgres_statement_create_result(struct da_statement *statement, struct da_result *result, const rt_char8 *sql);

RT_EXPORT rt_s da_postgres_statement_free(struct da_statement *statement);

RT_EXPORT rt_s da_postgres_statement_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size);

#endif /* DA_POSTGRES_STATEMENT_H */
