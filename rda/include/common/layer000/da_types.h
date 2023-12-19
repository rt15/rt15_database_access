#ifndef DA_TYPES_H
#define DA_TYPES_H

#include <rpr.h>

#define DA_IDENTIFIER_SIZE 64
#define DA_DB_SIZE 256

enum da_database_type {
	DA_DATABASE_TYPE_ORACLE,
	DA_DATABASE_TYPE_POSTGRES
};

struct da_last_error_message_provider;

typedef rt_s (*da_last_error_message_provider_append_t)(struct da_last_error_message_provider *last_error_message_provider, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size);

struct da_last_error_message_provider {
	da_last_error_message_provider_append_t append;
};

struct da_result;
struct da_statement;
struct da_connection;
struct da_data_source;
struct da_driver;

enum da_binding_type {
	DA_BINDING_TYPE_CHAR8,
	DA_BINDING_TYPE_N32
};

struct da_binding {
	enum da_binding_type type;
	rt_b is_null;
	union {
		struct {
			rt_char8 *buffer;
			rt_un buffer_capacity;
			rt_un buffer_size;
		} char8;
		struct {
			rt_n32 value;
		} n32;
	} u;
};

typedef rt_s (*da_result_bind_t)(struct da_result *result, struct da_binding *bindings, rt_un bindings_size);
typedef rt_s (*da_result_fetch_t)(struct da_result *result, rt_b *no_more_rows);
typedef rt_s (*da_result_free_t)(struct da_result *result);

struct da_result {
	da_result_bind_t bind;
	da_result_fetch_t fetch;
	da_result_free_t free;
	struct da_last_error_message_provider last_error_message_provider;
	struct da_statement *statement;
	struct da_binding *bindings;
	rt_un bindings_size;
	union {
		struct {
			void *pg_result;
			rt_n32 row_count;
			rt_n32 current_row;
		} postgres;
	} u;
};

typedef rt_s (*da_statement_execute_t)(struct da_statement *statement, const rt_char8 *sql, rt_un *row_count);
typedef rt_s (*da_statement_create_result_t)(struct da_statement *statement, struct da_result *result, const rt_char8 *sql);
typedef rt_s (*da_statement_free_t)(struct da_statement *statement);

struct da_statement {
	da_statement_execute_t execute;
	da_statement_create_result_t create_result;
	da_statement_free_t free;
	struct da_last_error_message_provider last_error_message_provider;
	struct da_connection *connection;
	union {
		struct {
			void *statement_handle;
		} oracle;
	} u;
};

typedef rt_s (*da_connection_open_t)(struct da_connection *connection);
typedef rt_s (*da_connection_create_statement_t)(struct da_connection *connection, struct da_statement *statement);
typedef rt_s (*da_connection_commit_t)(struct da_connection *connection);
typedef rt_s (*da_connection_rollback_t)(struct da_connection *connection);
typedef rt_s (*da_connection_free_t)(struct da_connection *connection);

struct da_connection {
	da_connection_open_t open;
	da_connection_create_statement_t create_statement;
	da_connection_commit_t commit;
	da_connection_rollback_t rollback;
	da_connection_free_t free;
	struct da_last_error_message_provider last_error_message_provider;
	struct da_data_source *data_source;
	rt_b auto_commit;
	rt_b opened;
	union {
		struct {
			void *error_handle;
			void *service_context_handle;
			void *session_handle;
			rt_b last_error_is_oracle;
			rt_n32 last_error_status;
			void *last_error_handle;
			rt_un32 last_error_handle_type;
		} oracle;
		struct {
			void *pg_conn;
			rt_b last_error_is_postgres;
		} postgres;
	} u;
};

typedef rt_s (*da_data_source_open_t)(struct da_data_source *data_source);
typedef rt_s (*da_data_source_create_connection_t)(struct da_data_source *data_source, struct da_connection *connection, rt_b auto_commit);
typedef rt_s (*da_data_source_free_t)(struct da_data_source *data_source);

struct da_data_source {
	da_data_source_open_t open;
	da_data_source_create_connection_t create_connection;
	da_data_source_free_t free;
	struct da_last_error_message_provider last_error_message_provider;
	struct da_driver *driver;
	rt_b opened;
	rt_char8 user_name[DA_IDENTIFIER_SIZE];
	rt_un user_name_size;
	rt_char8 password[DA_IDENTIFIER_SIZE];
	rt_un password_size;
	union {
		struct {
			void *error_handle;
			void *server_handle;
			rt_char8 db_link[DA_DB_SIZE];
			rt_un db_link_size;
			rt_b last_error_is_oracle;
			rt_n32 last_error_status;
			void *last_error_handle;
			rt_un32 last_error_handle_type;
		} oracle;
		struct {
			rt_char8 host_name[DA_IDENTIFIER_SIZE];
			rt_char8 port[DA_IDENTIFIER_SIZE];
			rt_char8 dbname[DA_DB_SIZE];
			const rt_char8 *keywords[6];
			const rt_char8 *values[6];
		} postgres;
	} u;
};

typedef rt_s (*da_driver_create_data_source_t)(struct da_driver *driver, struct da_data_source *data_source, const rt_char8 *host_name, rt_un port, const rt_char8 *database, const rt_char8 *user_name, const rt_char8 *password);
typedef rt_s (*da_driver_free_t)(struct da_driver *driver);

struct da_driver {
	da_driver_create_data_source_t create_data_source;
	da_driver_free_t free;
	struct da_last_error_message_provider last_error_message_provider;
	union {
		struct {
			void *environment_handle;
			rt_b last_error_is_oracle;
			rt_n32 last_error_status;
			void *last_error_handle;
			rt_un32 last_error_handle_type;
		} oracle;
	} u;
};

#endif /* DA_TYPES_H */