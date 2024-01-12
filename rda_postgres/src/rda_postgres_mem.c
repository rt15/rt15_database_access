#include <rpr.h>
#include <rpr_mem.h>

#ifdef RT_DEFINE_WINDOWS

/* Define the dll entry point. */
rt_n32 RT_STDCALL DllMainCRTStartup(RT_UNUSED rt_h dll_instance_handle, RT_UNUSED rt_un32 reason, RT_UNUSED void *reserved)
{
	return 1;
}

#endif
