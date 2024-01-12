cmake_minimum_required(VERSION 3.13.4)

project(rda_oracle)

set(PARENT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../..)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lib)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m64")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -ansi -pedantic -Wextra")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-long-long")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -finput-charset=cp1252")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mno-stack-arg-probe -fno-stack-check -fno-stack-protector")

string(REPLACE "-O3" "-O2" CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")

if(WIN32)
        add_compile_definitions(WIN32)
        add_link_options("-nostartfiles" "-nodefaultlibs" "-nostdlib" "-ffreestanding" "-Xlinker" "--stack=0x100000,0x100000")
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(BINARY_SUFFIX "d")
        add_compile_definitions(_DEBUG)
else()
        set(BINARY_SUFFIX "")
        add_compile_definitions(NDEBUG)
        add_link_options("-s")
endif()

include_directories(include)

include_directories(${PARENT_DIR}/rt15_portable_runtime/rpr/include)
include_directories(${PARENT_DIR}/rt15_database_access/rda/include)
if(WIN32)
include_directories(${PARENT_DIR}/libs/oracle/win64/include)
else()
include_directories(/usr/include/oracle/12.2/client64)
endif()

link_directories(${PARENT_DIR}/rt15_portable_runtime/rpr/lib)
if(WIN32)
link_directories(${PARENT_DIR}/libs/oracle/win64/lib/msvc)
else()
link_directories(/usr/lib/oracle/12.2/client64/lib)
endif()

link_libraries(rpr${BINARY_SUFFIX})
# On Linux:
# rpm -ivh oracle-instantclient12.2-basic-12.2.0.1.0-1.x86_64.rpm
# rpm -ivh oracle-instantclient12.2-devel-12.2.0.1.0-1.x86_64.rpm
if(WIN32)
link_libraries(oci)
else()
link_libraries(clntsh)
link_libraries(mql1)
link_libraries(ipc1)
link_libraries(nnz12)
link_libraries(ons)
link_libraries(clntshcore)
endif()

file(GLOB_RECURSE SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/*.c")

add_library(${PROJECT_NAME}${BINARY_SUFFIX} SHARED ${SOURCES})

# Remove the lib prefix from the dll name.
if(WIN32)
        set_property(TARGET ${PROJECT_NAME}${BINARY_SUFFIX} PROPERTY PREFIX "")
endif()
