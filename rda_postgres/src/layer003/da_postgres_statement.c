#include "layer003/da_postgres_statement.h"

#include "layer000/da_postgres_headers.h"
#include "layer001/da_postgres_utils.h"
#include "layer002/da_postgres_result.h"

/* select typname, oid from pg_type; */
#define DA_POSTGRES_STATEMENT_OID_INT4 23
#define DA_POSTGRES_STATEMENT_OID_TEXT 25

#define DA_POSTGRES_STATEMENT_FORMAT_TEXT 0
#define DA_POSTGRES_STATEMENT_FORMAT_BINARY 1

static rt_s da_postgres_statement_execute_internal(struct da_connection *connection, const rt_char8 *sql, PGresult **pg_result)
{
	PGconn *pg_conn = connection->u.postgres.pg_conn;
	PGresult *local_pg_result = RT_NULL;
	ExecStatusType status;
	rt_s ret;

	local_pg_result = PQexec(pg_conn, sql);

	if (RT_UNLIKELY(!local_pg_result)) {
		da_postgres_utils_set_with_last_error(connection);
		goto error;
	}

	status = PQresultStatus(local_pg_result);
	if (RT_UNLIKELY(status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK)) {
		da_postgres_utils_set_with_last_error(connection);
		goto error;
	}

	*pg_result = local_pg_result;

	ret = RT_OK;
free:
	return ret;

error:
	if (local_pg_result)
		PQclear(local_pg_result);

	ret = RT_FAILED;
	goto free;
}

static rt_s da_postgres_statement_get_row_count(PGresult *pg_result, rt_un *row_count)
{
	const rt_char8* affected_rows;
	rt_s ret;

	affected_rows = PQcmdTuples(pg_result);
	if (RT_UNLIKELY(!rt_char8_get_size(affected_rows))) {
		rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
		rt_last_error_message_set_with_last_error();
		goto error;
	}
	if (RT_UNLIKELY(!rt_char8_convert_to_un(affected_rows, row_count))) {
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_statement_execute(struct da_statement *statement, const rt_char8 *sql, rt_un *row_count)
{
	struct da_connection *connection = statement->connection;
	PGresult *pg_result;
	rt_b pg_result_created = RT_FALSE;
	rt_s ret;

	if (RT_UNLIKELY(!da_postgres_statement_execute_internal(connection, sql, &pg_result)))
		goto error;
	pg_result_created = RT_TRUE;

	if (row_count) {
		if (RT_UNLIKELY(!da_postgres_statement_get_row_count(pg_result, row_count)))
			goto error;
	}

	ret = RT_OK;
free:
	if (pg_result_created) {
		pg_result_created = RT_FALSE;
		/* PQclear returns void. */
		PQclear(pg_result);
	}
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_statement_select(struct da_statement *statement, struct da_result *result, const rt_char8 *sql)
{
	struct da_connection *connection = statement->connection;
	PGresult *pg_result;
	rt_b pg_result_created = RT_FALSE;
	rt_s ret;

	result->bind = &da_postgres_result_bind;
	result->fetch = &da_postgres_result_fetch;
	result->free = &da_postgres_result_free;

	result->statement = statement;

	if (RT_UNLIKELY(!da_postgres_statement_execute_internal(connection, sql, &pg_result)))
		goto error;
	pg_result_created = RT_TRUE;

	result->u.postgres.pg_result = pg_result;
	result->u.postgres.row_count = PQntuples(pg_result);
	result->u.postgres.current_row = 0;

	ret = RT_OK;
free:
	return ret;

error:
	if (pg_result_created)
		PQclear(pg_result);

	ret = RT_FAILED;
	goto free;
}

/**
 * Binary data must be in network byte order.
 */
static rt_s da_postgres_statement_prepare_value(enum da_binding_type *binding_types, rt_un binding_types_size, void ***batches, rt_un batches_size)
{
	rt_un row_index;
	rt_un column_index;
	rt_n32 *n32_value;
	rt_s ret;

	for (row_index = 0; row_index < batches_size; row_index++) {
		for (column_index = 0; column_index < binding_types_size; column_index++) {
			if (batches[row_index][column_index]) {
				switch (binding_types[column_index]) {
					case DA_BINDING_TYPE_CHAR8:
						break;
					case DA_BINDING_TYPE_N32:
						n32_value = batches[row_index][column_index];
						*n32_value = RT_MEMORY_SWAP_BYTES32(*n32_value);
						break;
					default:
						rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
						rt_last_error_message_set_with_last_error();
						goto error;
				}
			}
		}
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s da_postgres_statement_execute_prepared_internal(struct da_statement *statement, enum da_binding_type *binding_types, rt_un binding_types_size, void ***batches, rt_un batches_size, rt_un *row_count, PGresult **pg_result)
{
	struct da_connection *connection = statement->connection;
	PGconn *pg_conn = connection->u.postgres.pg_conn;
	rt_char8 *statement_name = statement->u.postgres.statement_name;
	const rt_char8 *prepared_sql = statement->prepared_sql;
	Oid *param_types;
	PGresult *prepare_pg_result = RT_NULL;
	ExecStatusType status;
	rt_b values_prepared = RT_FALSE;
	int *param_formats;
	int *param_lengths;
	const char **param_values;
	rt_un local_row_count;
	rt_un column_index;
	rt_un row_index;
	rt_s ret;

	if (RT_UNLIKELY(!batches_size)) {
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		rt_last_error_message_set_with_last_error();
		goto error;
	}

	/* We prepare it if it has not been prepared already. */
	if (!statement->prepared) {

		if (RT_UNLIKELY(!rt_static_heap_alloc((void**)&statement->u.postgres.param_formats, binding_types_size * sizeof(rt_n32)))) {
			rt_last_error_message_set_with_last_error();
			goto error;
		}
		if (RT_UNLIKELY(!rt_static_heap_alloc((void**)&statement->u.postgres.param_lengths, binding_types_size * sizeof(rt_n32)))) {
			rt_last_error_message_set_with_last_error();
			goto error;
		}

		/* We use param_formats temporarily for param_types. */
		param_types = (Oid*)statement->u.postgres.param_formats;

		for (column_index = 0; column_index < binding_types_size; column_index++) {
			switch (binding_types[column_index]) {
			case DA_BINDING_TYPE_CHAR8:
				param_types[column_index] = DA_POSTGRES_STATEMENT_OID_TEXT;
				break;
			case DA_BINDING_TYPE_N32:
				param_types[column_index] = DA_POSTGRES_STATEMENT_OID_INT4;
				break;
			default:
				rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
				rt_last_error_message_set_with_last_error();
				goto error;
			}
		}

		prepare_pg_result = PQprepare(pg_conn, statement_name, prepared_sql, binding_types_size, param_types);

		if (RT_UNLIKELY(!prepare_pg_result)) {
			da_postgres_utils_set_with_last_error(connection);
			goto error;
		}

		status = PQresultStatus(prepare_pg_result);
		if (RT_UNLIKELY(status != PGRES_COMMAND_OK)) {
			da_postgres_utils_set_with_last_error(connection);
			goto error;
		}
		statement->prepared = RT_TRUE;

		/* Prepare param_formats and param_length. */
		for (column_index = 0; column_index < binding_types_size; column_index++) {
			switch (binding_types[column_index]) {
				case DA_BINDING_TYPE_CHAR8:
					statement->u.postgres.param_formats[column_index] = DA_POSTGRES_STATEMENT_FORMAT_TEXT;
					/* param_length is ignored for test format. */
					break;
				case DA_BINDING_TYPE_N32:
					statement->u.postgres.param_formats[column_index] = DA_POSTGRES_STATEMENT_FORMAT_BINARY;
					statement->u.postgres.param_lengths[column_index] = 4;
					break;
				default:
					rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
					rt_last_error_message_set_with_last_error();
					goto error;
			}
		}
	}

	if (RT_UNLIKELY(!da_postgres_statement_prepare_value(binding_types, binding_types_size, batches, batches_size)))
		goto error;
	values_prepared = RT_TRUE;

	if (row_count)
		*row_count = 0;

	for (row_index = 0; row_index < batches_size; row_index++) {

		if (*pg_result) {
			/* PQclear returns void. */
			PQclear(*pg_result);
			*pg_result = RT_NULL;
		}

		param_formats = statement->u.postgres.param_formats;
		param_lengths = statement->u.postgres.param_lengths;
		param_values = (const char**)batches[row_index];

		*pg_result = PQexecPrepared(pg_conn, statement_name, binding_types_size, param_values, param_lengths, param_formats, 0);

		if (RT_UNLIKELY(!*pg_result)) {
			da_postgres_utils_set_with_last_error(connection);
			goto error;
		}

		status = PQresultStatus(*pg_result);
		if (RT_UNLIKELY(status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK)) {
			da_postgres_utils_set_with_last_error(connection);
			goto error;
		}

		if (row_count) {
			if (RT_UNLIKELY(!da_postgres_statement_get_row_count(*pg_result, &local_row_count)))
				goto error;

			*row_count += local_row_count;
		}
	}

	ret = RT_OK;
free:
	if (values_prepared) {
		values_prepared = RT_FALSE;
		if (RT_UNLIKELY(!da_postgres_statement_prepare_value(binding_types, binding_types_size, batches, batches_size) && ret))
			goto error;
	}
	if (prepare_pg_result) {
		/* PQclear returns void. */
		PQclear(prepare_pg_result);
	}
	return ret;
error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_statement_execute_prepared(struct da_statement *statement, enum da_binding_type *binding_types, rt_un binding_types_size, void ***batches, rt_un batches_size, rt_un *row_count)
{
	PGresult *pg_result = RT_NULL;
	rt_s ret;

	if (RT_UNLIKELY(!da_postgres_statement_execute_prepared_internal(statement, binding_types, binding_types_size, batches, batches_size, row_count, &pg_result)))
		goto error;

	ret = RT_OK;
free:
	if (pg_result) {
		/* PQclear returns void. */
		PQclear(pg_result);
	}
	return ret;
error:
	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_statement_select_prepared(struct da_statement *statement, struct da_result *result, enum da_binding_type *binding_types, rt_un binding_types_size, void **bindings)
{
	PGresult *pg_result = RT_NULL;
	rt_s ret;

	result->bind = &da_postgres_result_bind;
	result->fetch = &da_postgres_result_fetch;
	result->free = &da_postgres_result_free;

	result->statement = statement;

	if (RT_UNLIKELY(!da_postgres_statement_execute_prepared_internal(statement, binding_types, binding_types_size, &bindings, 1, RT_NULL, &pg_result)))
		goto error;

	result->u.postgres.pg_result = pg_result;
	result->u.postgres.row_count = PQntuples(pg_result);
	result->u.postgres.current_row = 0;

	ret = RT_OK;
free:
	return ret;

error:
	if (pg_result) {
		/* PQclear returns void. */
		PQclear(pg_result);
	}

	ret = RT_FAILED;
	goto free;
}

rt_s da_postgres_statement_free(struct da_statement *statement)
{
	rt_char8 *statement_name;
	rt_un statement_name_size;
	rt_char8 buffer[256];
	rt_un buffer_size;
	rt_s ret = RT_OK;

	if (statement->prepared) {
		buffer_size = 12;
		if (RT_UNLIKELY(!rt_char8_copy("deallocate \"", buffer_size, buffer, 256))) {
			/* Last error has been set by rt_char8_copy. */
			rt_last_error_message_set_with_last_error();
			ret = RT_FAILED;
		} else {
			statement_name = statement->u.postgres.statement_name;
			statement_name_size = statement->u.postgres.statement_name_size;

			if (RT_UNLIKELY(!rt_char8_append(statement_name, statement_name_size, buffer, 256, &buffer_size))) {
				/* Last error has been set by rt_char8_append. */
				rt_last_error_message_set_with_last_error();
				ret = RT_FAILED;
			} else {
				if (RT_UNLIKELY(!rt_char8_append_char('"', buffer, 256, &buffer_size))) {
					/* Last error has been set by rt_char8_append_char. */
					rt_last_error_message_set_with_last_error();
					ret = RT_FAILED;
				} else {
					if (RT_UNLIKELY(!statement->execute(statement, buffer, RT_NULL))) {
						/* Last error and last_error_is_postgres have been set by execute. */
						ret = RT_FAILED;
					}
				}
			}
		}
	}

	if (RT_UNLIKELY(!rt_static_heap_free((void**)&statement->u.postgres.param_lengths))) {
		/* Last error has been set by rt_static_heap_free. */
		rt_last_error_message_set_with_last_error();
		ret = RT_FAILED;
	}

	if (RT_UNLIKELY(!rt_static_heap_free((void**)&statement->u.postgres.param_formats))) {
		/* Last error has been set by rt_static_heap_free. */
		rt_last_error_message_set_with_last_error();
		ret = RT_FAILED;
	}

	return ret;
}
