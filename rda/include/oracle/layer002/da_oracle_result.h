#ifndef DA_ORACLE_RESULT_H
#define DA_ORACLE_RESULT_H

#include "common/layer000/da_types.h"

RT_EXPORT rt_s da_oracle_result_bind(struct da_result *result, struct da_binding *bindings, rt_un bindings_size);
RT_EXPORT rt_s da_oracle_result_fetch(struct da_result *result, rt_b *end_of_rows);
RT_EXPORT rt_s da_oracle_result_free(struct da_result *result);

RT_EXPORT rt_s da_oracle_result_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size);

#endif /* DA_ORACLE_RESULT_H */
