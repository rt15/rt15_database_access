#include "postgres/layer002/da_postgres_result.h"

#include "postgres/layer000/da_postgres_headers.h"
#include "postgres/layer001/da_postgres_utils.h"

rt_s da_postgres_result_bind(struct da_result *result, struct da_binding *bindings, rt_un bindings_size)
{
	result->bindings = bindings;
	result->bindings_size = bindings_size;
	return RT_OK;
}

rt_s da_postgres_result_fetch(struct da_result *result, rt_b *no_more_rows)
{
	struct da_connection *connection = result->statement->connection;
	struct da_binding *bindings = result->bindings;
	rt_un bindings_size = result->bindings_size;
	PGresult *pg_result = (PGresult*)result->u.postgres.pg_result;
	rt_n32 current_row = result->u.postgres.current_row;
	const rt_char8 *value;
	int value_size;
	rt_n n;
	rt_un i;
	rt_s ret;

	if (current_row >= result->u.postgres.row_count) {
		*no_more_rows = RT_TRUE;
	} else {
		*no_more_rows = RT_FALSE;

		for (i = 0; i < bindings_size; i++) {
			if (PQgetisnull(pg_result, current_row, i)) {
				bindings[i].is_null = RT_TRUE;
			} else {
				bindings[i].is_null = RT_FALSE;

				value = PQgetvalue(pg_result, current_row, i);
				if (RT_UNLIKELY(!value)) {
					rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
					connection->u.postgres.last_error_is_postgres = RT_TRUE;
					goto error;
				}
				switch (bindings[i].type) {
				case DA_BINDING_TYPE_CHAR8:
					value_size = PQgetlength(pg_result, current_row, i);
					if (RT_UNLIKELY(!rt_char8_copy(value, value_size, bindings[i].u.char8.buffer, bindings[i].u.char8.buffer_capacity)))
						goto error;
					bindings[i].u.char8.buffer_size = value_size;
					break;
				case DA_BINDING_TYPE_N32:
					if (RT_UNLIKELY(!rt_char8_convert_to_n(value, &n)))
						goto error;
					if (RT_UNLIKELY(n < RT_TYPE_MIN_N32 || n > RT_TYPE_MAX_N32)) {
						rt_error_set_last(RT_ERROR_ARITHMETIC_OVERFLOW);
						goto error;
					}
					bindings[i].u.n32.value = (rt_n32)n;
					break;
				default:
					rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
					connection->u.postgres.last_error_is_postgres = RT_FALSE;
				}
			}
		}

		result->u.postgres.current_row++;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_result_free(struct da_result *result)
{
	PGresult *pg_result = (PGresult*)result->u.postgres.pg_result;

	/* PQclear returns void. */
	PQclear(pg_result);

	return RT_OK;
}

rt_s da_postgres_result_append_last_error_message(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	struct da_result *result = RT_MEMORY_CONTAINER_OF(last_error_message_provider, struct da_result, last_error_message_provider);
	struct da_statement *statement = result->statement;
	struct da_connection *connection = statement->connection;

	return da_postres_utils_append_error_message(connection->u.postgres.last_error_is_postgres, connection, buffer, buffer_capacity, buffer_size);
}
