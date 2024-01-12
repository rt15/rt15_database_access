#ifndef DA_POSTGRES_STATEMENT_H
#define DA_POSTGRES_STATEMENT_H

#include "common/layer000/da_types.h"

RT_EXPORT rt_s da_postgres_statement_execute(struct da_statement *statement, const rt_char8 *sql, rt_un *row_count);

RT_EXPORT rt_s da_postgres_statement_select(struct da_statement *statement, struct da_result *result, const rt_char8 *sql);

RT_EXPORT rt_s da_postgres_statement_execute_prepared(struct da_statement *statement, enum da_binding_type *binding_types, rt_un binding_types_size, void ***batches, rt_un batches_size, rt_un *row_count);

RT_EXPORT rt_s da_postgres_statement_select_prepared(struct da_statement *statement, struct da_result *result, enum da_binding_type *binding_types, rt_un binding_types_size, void **bindings);

RT_EXPORT rt_s da_postgres_statement_free(struct da_statement *statement);

#endif /* DA_POSTGRES_STATEMENT_H */
