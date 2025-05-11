#include "layer002/da_postgres_result.h"

#include "layer000/da_postgres_headers.h"
#include "layer001/da_postgres_utils.h"

rt_s da_postgres_result_bind(struct da_result *result, struct da_binding *bindings, rt_un bindings_size)
{
	result->bindings = bindings;
	result->bindings_size = bindings_size;
	return RT_OK;
}

rt_s da_postgres_result_fetch(struct da_result *result, rt_b *end_of_rows)
{
	struct da_connection *connection = result->statement->connection;
	struct da_binding *bindings = result->bindings;
	rt_un bindings_size = result->bindings_size;
	PGresult *pg_result = (PGresult*)result->u.postgres.pg_result;
	rt_n32 current_row = result->u.postgres.current_row;
	rt_un column_index;
	const rt_char8 *value;
	int value_size;
	rt_n n;
	rt_s ret = RT_FAILED;

	if (current_row >= result->u.postgres.row_count) {
		*end_of_rows = RT_TRUE;
	} else {
		*end_of_rows = RT_FALSE;

		for (column_index = 0; column_index < bindings_size; column_index++) {
			if (PQgetisnull(pg_result, current_row, column_index)) {
				bindings[column_index].is_null = RT_TRUE;
			} else {
				bindings[column_index].is_null = RT_FALSE;

				value = PQgetvalue(pg_result, current_row, column_index);
				if (RT_UNLIKELY(!value)) {
					da_postgres_utils_set_with_last_error(connection);
					goto end;
				}
				switch (bindings[column_index].type) {
				case DA_BINDING_TYPE_CHAR8:
					value_size = PQgetlength(pg_result, current_row, column_index);
					if (RT_UNLIKELY(!rt_char8_copy(value, value_size, bindings[column_index].u.char8.buffer, bindings[column_index].u.char8.buffer_capacity))) {
						rt_last_error_message_set_with_last_error();
						goto end;
					}
					bindings[column_index].u.char8.buffer_size = value_size;
					break;
				case DA_BINDING_TYPE_N32:
					if (RT_UNLIKELY(!rt_char8_convert_to_n(value, &n))) {
						rt_last_error_message_set_with_last_error();
						goto end;
					}
					if (RT_UNLIKELY(n < RT_TYPE_MIN_N32 || n > RT_TYPE_MAX_N32)) {
						rt_error_set_last(RT_ERROR_ARITHMETIC_OVERFLOW);
						rt_last_error_message_set_with_last_error();
						goto end;
					}
					bindings[column_index].u.n32.value = (rt_n32)n;
					break;
				default:
					rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
					rt_last_error_message_set_with_last_error();
					goto end;
				}
			}
		}

		result->u.postgres.current_row++;
	}

	ret = RT_OK;
end:
	return ret;
}

rt_s da_postgres_result_free(struct da_result *result)
{
	PGresult *pg_result = (PGresult*)result->u.postgres.pg_result;

	/* PQclear returns void. */
	PQclear(pg_result);

	return RT_OK;
}
