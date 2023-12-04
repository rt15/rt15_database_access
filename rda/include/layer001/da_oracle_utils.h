#ifndef DA_ORACLE_UTILS_H
#define DA_ORACLE_UTILS_H

#include <rpr.h>

RT_EXPORT rt_s da_oracle_utils_append_error_message(rt_n32 error_status, void *error_handle, rt_un32 error_handle_type, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size);

#endif /* DA_ORACLE_UTILS_H */
