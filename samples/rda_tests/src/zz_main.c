#include <rpr.h>
#include <rpr_main.h>

#define ZZ_SETTING_SIZE 256

struct zz_settings {
	const rt_char *host_name;
	const rt_char *port;
	const rt_char *database;
	const rt_char *user_name;
	const rt_char *password;
};

rt_s zz_test_rda(const rt_char8 *host_name, rt_un port, const rt_char8 *database, const rt_char8 *user_name, const rt_char8 *password);

static void zz_unknown_parameter(const rt_char *parameter_name, rt_un parameter_name_size)
{
	rt_char buffer[RT_CHAR_HALF_BIG_STRING_SIZE];
	rt_un buffer_size;

	rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);

	buffer_size = 18;
	if (RT_UNLIKELY(!rt_char_copy(_R("Unknown parameter "), buffer_size, buffer, RT_CHAR_HALF_BIG_STRING_SIZE))) return;
	if (RT_UNLIKELY(!rt_char_append(parameter_name, parameter_name_size, buffer, RT_CHAR_HALF_BIG_STRING_SIZE, &buffer_size))) return;
	if (RT_UNLIKELY(!rt_char_append_char(_R('.'), buffer, RT_CHAR_HALF_BIG_STRING_SIZE, &buffer_size))) return;

	rt_console_write_error_with_size(buffer, buffer_size);
}

static rt_s zz_handle_parameter(const rt_char *parameter_name, const rt_char *value, const rt_char **setting)
{
	rt_char buffer[RT_CHAR_HALF_BIG_STRING_SIZE];
	rt_un buffer_size;
	rt_s ret;

	if (RT_UNLIKELY(*setting)) {
		buffer_size = 11;
		if (RT_UNLIKELY(!rt_char_copy(_R("Duplicated "), buffer_size, buffer, RT_CHAR_HALF_BIG_STRING_SIZE))) goto error;
		if (RT_UNLIKELY(!rt_char_append(parameter_name, rt_char_get_size(parameter_name), buffer, RT_CHAR_HALF_BIG_STRING_SIZE, &buffer_size))) goto error;
		if (RT_UNLIKELY(!rt_char_append(_R(" parameter."), 11, buffer, RT_CHAR_HALF_BIG_STRING_SIZE, &buffer_size))) goto error;
		if (RT_UNLIKELY(!rt_console_write_error_with_size(buffer, buffer_size))) goto error;
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		goto error;
	}

	if (RT_UNLIKELY(!value)) {
		buffer_size = 22;
		if (RT_UNLIKELY(!rt_char_copy(_R("No value provided for "), buffer_size, buffer, RT_CHAR_HALF_BIG_STRING_SIZE))) goto error;
		if (RT_UNLIKELY(!rt_char_append(parameter_name, rt_char_get_size(parameter_name), buffer, RT_CHAR_HALF_BIG_STRING_SIZE, &buffer_size))) goto error;
		if (RT_UNLIKELY(!rt_char_append(_R(" parameter."), 11, buffer, RT_CHAR_HALF_BIG_STRING_SIZE, &buffer_size))) goto error;
		if (RT_UNLIKELY(!rt_console_write_error_with_size(buffer, buffer_size))) goto error;
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		goto error;
	}

	*setting = value;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s zz_handle_short_parameter(rt_char short_option, const rt_char *value, rt_b valid, struct zz_settings *settings)
{
	rt_s ret;

	if (!valid) {
		zz_unknown_parameter(&short_option, 1);
		goto error;
	}

	switch (short_option) {
	case _R('h'):
		if (RT_UNLIKELY(!zz_handle_parameter(_R("host-name"), value, &settings->host_name)))
			goto error;
		break;
	case _R('P'):
		if (RT_UNLIKELY(!zz_handle_parameter(_R("port"), value, &settings->port)))
			goto error;
		break;
	case _R('d'):
		if (RT_UNLIKELY(!zz_handle_parameter(_R("database"), value, &settings->database)))
			goto error;
		break;
	case _R('u'):
		if (RT_UNLIKELY(!zz_handle_parameter(_R("user-name"), value, &settings->user_name)))
			goto error;
		break;
	case _R('p'):
		if (RT_UNLIKELY(!zz_handle_parameter(_R("password"), value, &settings->password)))
			goto error;
		break;
	default:
		zz_unknown_parameter(&short_option, 1);
		goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s zz_handle_long_parameter(const rt_char *long_option, const rt_char *value, rt_b valid, struct zz_settings *settings)
{
	rt_un long_option_size = rt_char_get_size(long_option);
	rt_s ret;

	if (!valid) {
		zz_unknown_parameter(long_option, long_option_size);
		goto error;
	}

	if (rt_char_equals(long_option, long_option_size, _R("host-name"), 9)) {
		if (RT_UNLIKELY(!zz_handle_parameter(long_option, value, &settings->host_name)))
			goto error;
	} else if (rt_char_equals(long_option, long_option_size, _R("port"), 4)) {
		if (RT_UNLIKELY(!zz_handle_parameter(long_option, value, &settings->port)))
			goto error;
	} else if (rt_char_equals(long_option, long_option_size, _R("database"), 8)) {
		if (RT_UNLIKELY(!zz_handle_parameter(long_option, value, &settings->database)))
			goto error;
	} else if (rt_char_equals(long_option, long_option_size, _R("user-name"), 9)) {
		if (RT_UNLIKELY(!zz_handle_parameter(long_option, value, &settings->user_name)))
			goto error;
	} else if (rt_char_equals(long_option, long_option_size, _R("password"), 8)) {
		if (RT_UNLIKELY(!zz_handle_parameter(long_option, value, &settings->password)))
			goto error;
	} else {
		zz_unknown_parameter(long_option, long_option_size);
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s zz_command_line_args_parse_callback(enum rt_command_line_args_type arg_type, rt_b valid, rt_char short_option, RT_UNUSED const rt_char *long_option,
						RT_UNUSED enum rt_command_line_args_value_cardinality value_cardinality, const rt_char *value, void *context)
{
	struct zz_settings *settings = (struct zz_settings*)context;
	rt_s ret;

	switch (arg_type){
	case RT_COMMAND_LINE_ARGS_TYPE_SHORT:
		if (RT_UNLIKELY(!zz_handle_short_parameter(short_option, value, valid, settings)))
			goto error;
		break;
	case RT_COMMAND_LINE_ARGS_TYPE_LONG:
		if (RT_UNLIKELY(!zz_handle_long_parameter(long_option, value, valid, settings)))
			goto error;
		break;
	case RT_COMMAND_LINE_ARGS_TYPE_NON_OPTION:
	case RT_COMMAND_LINE_ARGS_TYPE_END_OF_OPTIONS:
		/* The callback should not be called with these. */
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		goto error;
		break;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s zz_prepare_arg(const rt_char *parameter_name, const rt_char *arg, rt_char8 *arg_buffer)
{
	rt_char buffer[RT_CHAR_HALF_BIG_STRING_SIZE];
	rt_un buffer_size;
	rt_char8 *output;
	rt_un output_size;
	rt_s ret;

	if (RT_UNLIKELY(!arg)) {
		buffer_size = 8;
		if (RT_UNLIKELY(!rt_char_copy(_R("Missing "), buffer_size, buffer, RT_CHAR_HALF_BIG_STRING_SIZE))) goto error;
		if (RT_UNLIKELY(!rt_char_append(parameter_name, rt_char_get_size(parameter_name), buffer, RT_CHAR_HALF_BIG_STRING_SIZE, &buffer_size))) goto error;
		if (RT_UNLIKELY(!rt_char_append(_R(" parameter."), 11, buffer, RT_CHAR_HALF_BIG_STRING_SIZE, &buffer_size))) goto error;
		if (RT_UNLIKELY(!rt_console_write_error_with_size(buffer, buffer_size))) goto error;
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		goto error;
	}

	if (RT_UNLIKELY(!rt_encoding_encode(arg, rt_char_get_size(arg), RT_ENCODING_SYSTEM_DEFAULT, arg_buffer, ZZ_SETTING_SIZE, RT_NULL, RT_NULL, &output, &output_size, RT_NULL)))
		goto error;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s zz_parse_args(rt_un argc, const rt_char *argv[])
{
	const rt_char *long_options_with_arg[6];
	rt_un non_option_index;
	struct zz_settings settings;
	rt_char8 host_name[ZZ_SETTING_SIZE];
	rt_un port;
	rt_char8 database[ZZ_SETTING_SIZE];
	rt_char8 user_name[ZZ_SETTING_SIZE];
	rt_char8 password[ZZ_SETTING_SIZE];
	rt_char buffer[RT_CHAR_HALF_BIG_STRING_SIZE];
	rt_un buffer_size;
	rt_s ret;

	long_options_with_arg[0] = _R("host-name");
	long_options_with_arg[1] = _R("port");
	long_options_with_arg[2] = _R("database");
	long_options_with_arg[3] = _R("user-name");
	long_options_with_arg[4] = _R("password");
	long_options_with_arg[5] = RT_NULL;

	rt_memory_zero(&settings, sizeof(struct zz_settings));

	if (RT_UNLIKELY(!rt_command_line_args_parse(&argc, argv, &zz_command_line_args_parse_callback, &settings, RT_NULL, RT_NULL,
						    _R("hPdup"), RT_NULL, RT_NULL, long_options_with_arg, &non_option_index)))
		goto error;

	if (RT_UNLIKELY(!settings.port)) {
		rt_console_write_error(_R("Missing port parameter."));
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		goto error;
	}

	if (RT_UNLIKELY(!rt_char_convert_to_un(settings.port, &port) || port > 65535)) {
		buffer_size = 1;
		if (RT_UNLIKELY(!rt_char_copy(_R("\""), buffer_size, buffer, RT_CHAR_HALF_BIG_STRING_SIZE))) goto error;
		if (RT_UNLIKELY(!rt_char_append(settings.port, rt_char_get_size(settings.port), buffer, RT_CHAR_HALF_BIG_STRING_SIZE, &buffer_size))) goto error;
		if (RT_UNLIKELY(!rt_char_append(_R("\" is not a valid port."), 22, buffer, RT_CHAR_HALF_BIG_STRING_SIZE, &buffer_size))) goto error;
		if (RT_UNLIKELY(!rt_console_write_error_with_size(buffer, buffer_size))) goto error;
		goto error;
	}

	if (RT_UNLIKELY(!zz_prepare_arg(_R("host-name"), settings.host_name, host_name))) goto error;
	if (RT_UNLIKELY(!zz_prepare_arg(_R("database"), settings.database, database))) goto error;
	if (RT_UNLIKELY(!zz_prepare_arg(_R("user-name"), settings.user_name, user_name))) goto error;
	if (RT_UNLIKELY(!zz_prepare_arg(_R("password"), settings.password, password))) goto error;

	if (RT_UNLIKELY(non_option_index != RT_TYPE_MAX_UN)) {
		rt_console_write_error(_R("Non options are not supported."));
		rt_error_set_last(RT_ERROR_BAD_ARGUMENTS);
		goto error;
	}

	if (RT_UNLIKELY(!zz_test_rda(host_name, port, database, user_name, password)))
		goto error;

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

static rt_s zz_main(rt_un argc, const rt_char *argv[])
{
	rt_b display_help = RT_FALSE;
	const rt_char *arg;
	rt_un arg_size;
	rt_s ret;

	if (argc == 2) {
		arg = argv[1];
		arg_size = rt_char_get_size(arg);
		if (rt_char_equals(arg, arg_size, _R("--help"), 6) ||
		    rt_char_equals(arg, arg_size, _R("-h"), 2) ||
		    rt_char_equals(arg, arg_size, _R("/?"), 2)) {
			display_help = RT_TRUE;
		}
	}

	if (display_help) {
		if (RT_UNLIKELY(!rt_console_write_string(_R("rda_tests <-h|--host-name> HOST_NAME <-P|--port> PORT <-d|--database> DATABASE <-u|--user-name> USER_NAME <-p|--password> PASSWORD\n"))))
			goto error;

	} else {
		if (RT_UNLIKELY(!zz_parse_args(argc, argv)))
			goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}

rt_un16 rpr_main(rt_un argc, const rt_char *argv[])
{
	int ret;

	if (zz_main(argc, argv))
		ret = 0;
	else
		ret = 1;
	return ret;
}
