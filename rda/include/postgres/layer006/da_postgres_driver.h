#ifndef DA_POSTGRES_DRIVER_H
#define DA_POSTGRES_DRIVER_H

#include "common/layer000/da_types.h"

RT_EXPORT rt_s da_postgres_driver_create(struct da_driver *driver);

RT_EXPORT rt_s da_postgres_driver_create_data_source(struct da_driver *driver, struct da_data_source *data_source, const rt_char8 *host_name, rt_un port, const rt_char8 *database, const rt_char8 *user_name, const rt_char8 *password);

RT_EXPORT rt_s da_postgres_driver_free(struct da_driver *driver);

RT_EXPORT rt_s da_postgres_driver_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size);

#endif /* DA_POSTGRES_DRIVER_H */
