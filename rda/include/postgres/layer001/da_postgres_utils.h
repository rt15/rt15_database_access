#ifndef DA_POSTGRES_UTILS_H
#define DA_POSTGRES_UTILS_H

#include <rpr.h>

#include "common/layer000/da_types.h"

RT_EXPORT rt_s da_postgres_utils_append_error_message(struct da_connection *connection, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size);

#endif /* DA_POSTGRES_UTILS_H */
