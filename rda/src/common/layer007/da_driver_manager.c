#include "common/layer007/da_driver_manager.h"

#include "oracle/layer006/da_oracle_driver.h"
#include "postgres/layer006/da_postgres_driver.h"

rt_s da_driver_manager_create_driver(struct da_driver *driver, enum da_database_type database_type)
{
	rt_s ret;

	switch (database_type) {
	case DA_DATABASE_TYPE_ORACLE:
		if (RT_UNLIKELY(!da_oracle_driver_create(driver)))
			goto error;
		break;
	case DA_DATABASE_TYPE_POSTGRES:
		if (RT_UNLIKELY(!da_postgres_driver_create(driver)))
			goto error;
		break;
	default:
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}
