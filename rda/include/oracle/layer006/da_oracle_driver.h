#ifndef DA_ORACLE_DRIVER_H
#define DA_ORACLE_DRIVER_H

#include "common/layer000/da_types.h"

RT_EXPORT rt_s da_oracle_driver_create(struct da_driver *driver);

RT_EXPORT rt_s da_oracle_driver_create_data_source(struct da_driver *driver, struct da_data_source *data_source, const rt_char8 *host_name, rt_un port, const rt_char8 *database, const rt_char8 *user_name, const rt_char8 *password);

RT_EXPORT rt_s da_oracle_driver_free(struct da_driver *driver);

#endif /* DA_ORACLE_DRIVER_H */
