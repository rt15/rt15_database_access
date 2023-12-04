#ifndef DA_DRIVER_MANAGER_H
#define DA_DRIVER_MANAGER_H

#include "layer000/da_types.h"

RT_EXPORT rt_s da_driver_manager_create_driver(struct da_driver *driver, enum da_database_type database_type);

#endif /* DA_DRIVER_MANAGER_H */
