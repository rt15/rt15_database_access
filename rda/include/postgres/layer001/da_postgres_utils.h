#ifndef DA_POSTGRES_UTILS_H
#define DA_POSTGRES_UTILS_H

#include <rpr.h>

#include "common/layer000/da_types.h"

/**
 * Can be used to execute a sql command that does not return a result set.
 */
RT_EXPORT rt_s da_postgres_utils_execute(struct da_connection *connection, const rt_char8 *sql);

RT_EXPORT rt_s da_postres_utils_append_error_message(rt_b is_postgres, struct da_connection *connection, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size);

#endif /* DA_POSTGRES_UTILS_H */
