#include "layer001/da_oracle_utils.h"

#include "layer000/da_oracle_headers.h"

rt_s da_oracle_utils_set_with_last_error(rt_n32 error_status, void *error_handle, rt_un32 error_handle_type)
{
	sb4 errcode = 0;
	rt_char8 buffer8[RT_CHAR8_HALF_BIG_STRING_SIZE];
	rt_un buffer8_size;
	rt_char buffer[RT_CHAR_HALF_BIG_STRING_SIZE];
	rt_char *output;
	rt_un output_size;
	rt_char *status_name;
	rt_s ret;

	if (error_status == OCI_ERROR && error_handle) {

		if (RT_UNLIKELY(OCIErrorGet(error_handle, 1, RT_NULL, &errcode, (OraText*)buffer8, RT_CHAR8_HALF_BIG_STRING_SIZE, error_handle_type) != OCI_SUCCESS)) {
			rt_error_set_last(RT_ERROR_FUNCTION_FAILED);
			rt_last_error_message_set_with_last_error();
			goto error;
		}

		buffer8_size = rt_char8_get_size(buffer8);
		if (RT_UNLIKELY(!rt_encoding_decode(buffer8, buffer8_size, RT_ENCODING_SYSTEM_DEFAULT, buffer, RT_CHAR_HALF_BIG_STRING_SIZE, RT_NULL, RT_NULL, &output, &output_size, RT_NULL))) {
			rt_last_error_message_set_with_last_error();
			goto error;
		}

		if (RT_UNLIKELY(!rt_last_error_message_set(buffer))) {
			rt_last_error_message_set_with_last_error();
			goto error;
		}

	} else {
		switch (error_status) {
		case OCI_SUCCESS:
			status_name = _R("OCI_SUCCESS");
			break;
		case OCI_SUCCESS_WITH_INFO:
			status_name = _R("OCI_SUCCESS_WITH_INFO");
			break;
		case OCI_RESERVED_FOR_INT_USE:
			status_name = _R("OCI_RESERVED_FOR_INT_USE");
			break;
		case OCI_NO_DATA:
			status_name = _R("OCI_NO_DATA");
			break;
		case OCI_ERROR:
			status_name = _R("OCI_ERROR");
			break;
		case OCI_INVALID_HANDLE:
			status_name = _R("OCI_INVALID_HANDLE");
			break;
		case OCI_NEED_DATA:
			status_name = _R("OCI_NEED_DATA");
			break;
		case OCI_STILL_EXECUTING:
			status_name = _R("OCI_STILL_EXECUTING");
			break;
		default:
			status_name = _R("Unknown status");
			break;
		}

		if (RT_UNLIKELY(!rt_last_error_message_set(status_name))) {
			rt_last_error_message_set_with_last_error();
			goto error;
		}
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}
