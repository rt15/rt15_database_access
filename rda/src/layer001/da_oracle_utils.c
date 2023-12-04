#include "layer001/da_oracle_utils.h"

#include "layer000/da_oracle_headers.h"

rt_s da_oracle_utils_append_error_message(rt_n32 error_status, void *error_handle, rt_un32 error_handle_type, rt_char *buffer, rt_un buffer_capacity, rt_un *buffer_size)
{
	sb4 errcode = 0;
	rt_char8 buffer8[RT_CHAR8_BIG_STRING_SIZE];
	rt_char *output;
	rt_un output_size;
	rt_char *status_name;
	rt_s ret;

	if (error_status == OCI_ERROR && error_handle) {

		if (RT_UNLIKELY(OCIErrorGet(error_handle, 1, RT_NULL, &errcode, (OraText*)buffer8, RT_CHAR8_BIG_STRING_SIZE, error_handle_type) != OCI_SUCCESS))
			goto error;

		if (RT_UNLIKELY(!rt_encoding_decode(buffer8, rt_char8_get_size(buffer8), RT_ENCODING_SYSTEM_DEFAULT, &buffer[*buffer_size], buffer_capacity - *buffer_size, RT_NULL, RT_NULL, &output, &output_size, RT_NULL)))
			goto error;

		*buffer_size += output_size;

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
		if (RT_UNLIKELY(!rt_char_append(status_name, rt_char_get_size(status_name), buffer, buffer_capacity, buffer_size)))
			goto error;
	}

	ret = RT_OK;
free:
	return ret;

error:
	ret = RT_FAILED;
	goto free;
}
