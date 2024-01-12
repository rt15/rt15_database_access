#ifndef DA_ORACLE_DATA_SOURCE_H
#define DA_ORACLE_DATA_SOURCE_H

#include "rda.h"

RT_EXPORT rt_s da_oracle_data_source_open(struct da_data_source *data_source);

RT_EXPORT rt_s da_oracle_data_source_create_connection(struct da_data_source *data_source, struct da_connection *connection, rt_b auto_commit);

RT_EXPORT rt_s da_oracle_data_source_free(struct da_data_source *data_source);

#endif /* DA_ORACLE_DATA_SOURCE_H */
