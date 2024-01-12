#ifndef DA_ORACLE_RESULT_H
#define DA_ORACLE_RESULT_H

#include "common/layer000/da_types.h"

RT_EXPORT rt_s da_oracle_result_bind(struct da_result *result, struct da_binding *bindings, rt_un bindings_size);
RT_EXPORT rt_s da_oracle_result_fetch(struct da_result *result, rt_b *end_of_rows);
RT_EXPORT rt_s da_oracle_result_free(struct da_result *result);

#endif /* DA_ORACLE_RESULT_H */
