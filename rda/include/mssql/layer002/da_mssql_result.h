#ifndef DA_MSSQL_RESULT_H
#define DA_MSSQL_RESULT_H

#include "common/layer000/da_types.h"

RT_EXPORT rt_s da_mssql_result_bind(struct da_result *result, struct da_binding *bindings, rt_un bindings_size);
RT_EXPORT rt_s da_mssql_result_fetch(struct da_result *result, rt_b *end_of_rows);
RT_EXPORT rt_s da_mssql_result_free(struct da_result *result);

#endif /* DA_MSSQL_RESULT_H */
