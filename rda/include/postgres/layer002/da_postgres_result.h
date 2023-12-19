#ifndef DA_POSTGRES_RESULT_H
#define DA_POSTGRES_RESULT_H

#include "common/layer000/da_types.h"

RT_EXPORT rt_s da_postgres_result_bind(struct da_result *result, struct da_binding *bindings, rt_un bindings_size);
RT_EXPORT rt_s da_postgres_result_fetch(struct da_result *result, rt_b *no_more_rows);
RT_EXPORT rt_s da_postgres_result_free(struct da_result *result);

RT_EXPORT rt_s da_postgres_result_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size);

#endif /* DA_POSTGRES_RESULT_H */
