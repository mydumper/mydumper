cmake_minimum_required(VERSION 2.8.12)
project(mydumper)
set(VERSION 0.15.2-6)
set(ARCHIVE_NAME "${CMAKE_PROJECT_NAME}-${VERSION}")

#Required packages
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/modules)
find_package(MySQL)
find_package(ZLIB)
find_package(GLIB2)
find_package(PCRE)
# find_package(JeMalloc)

#if (NOT JEMALLOC_FOUND)
#    MESSAGE(WARNING "Could not find JeMalloc library")
#endif ()

if (NOT MYSQL_FOUND)
    MESSAGE(FATAL_ERROR "Could not find MySQL or MariaDB client libraries")
endif ()

option(BUILD_DOCS "Build the documentation" ON)

if (BUILD_DOCS)
  add_subdirectory(docs)
endif (BUILD_DOCS)

if (PC_GLIB2_VERSION VERSION_GREATER "2.51")
  option(WITH_GLIB_uuid_string_random "uuid_string_random not available" ON)
endif (PC_GLIB2_VERSION VERSION_GREATER "2.51")

#if (PC_GLIB2_VERSION VERSION_LESS "2.68")
#	MESSAGE(FATAL_ERROR "GLIB version lower than 2.68")
#endif (PC_GLIB2_VERSION VERSION_LESS "2.68")

option(WITH_SSL "Build SSL support" ON)
if (MARIADB_FOUND AND NOT MARIADB_SSL AND WITH_SSL)
    message(WARNING "MariaDB was not build with SSL so cannot turn SSL on")
    set(WITH_SSL OFF)
endif()

set(CMAKE_C_FLAGS "-std=gnu99 -Wall -Wno-deprecated-declarations -Wunused -Wwrite-strings -Wno-strict-aliasing -Wextra -Wshadow -g -Werror ${MYSQL_CFLAGS}")
include_directories(${MYDUMPER_SOURCE_DIR} ${MYSQL_INCLUDE_DIR} ${GLIB2_INCLUDE_DIR} ${PCRE_INCLUDE_DIR} ${ZLIB_INCLUDE_DIRS} )

OPTION(WITH_ASAN "Build with ASAN" OFF)
OPTION(WITH_TSAN "Build with TSAN" OFF)

IF(WITH_ASAN AND WITH_TSAN)
    MESSAGE(FATAL_ERROR "Cannot use ASAN and TSAN at the same time")
ENDIF()

IF(WITH_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
#    add_link_options(-fsanitize=address -fno-omit-frame-pointer)
    set (CMAKE_EXE_LINKER_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
    set (CMAKE_SHARED_LINKER_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
    set (CMAKE_STATIC_LINKER_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
ELSEIF(WITH_TSAN)
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
#    add_link_options(-fsanitize=thread -fno-omit-frame-pointer)
    set (CMAKE_EXE_LINKER_FLAGS "-fsanitize=thread -fno-omit-frame-pointer")
    set (CMAKE_SHARED_LINKER_FLAGS "-fsanitize=thread -fno-omit-frame-pointer")
    set (CMAKE_STATIC_LINKER_FLAGS "-fsanitize=thread -fno-omit-frame-pointer")
ELSE()
#    add_compile_options(-O3)
#    add_link_options(-O3)
#    set (CMAKE_SHARED_LINKER_FLAGS "-O3")
ENDIF()

if (NOT CMAKE_INSTALL_PREFIX)
  SET(CMAKE_INSTALL_PREFIX "/usr/local" CACHE STRING "Install path" FORCE)
endif (NOT CMAKE_INSTALL_PREFIX)
MARK_AS_ADVANCED(CMAKE)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_SOURCE_DIR}/src/config.h )
SET( SHARED_SRCS src/server_detect.c src/connection.c src/logging.c src/set_verbose.c src/common.c src/tables_skiplist.c src/regex.c )
SET( MYDUMPER_SRCS src/mydumper.c ${SHARED_SRCS} src/mydumper_pmm_thread.c src/mydumper_start_dump.c src/mydumper_jobs.c src/mydumper_common.c src/mydumper_stream.c src/mydumper_database.c src/mydumper_working_thread.c src/mydumper_daemon_thread.c src/mydumper_exec_command.c src/mydumper_masquerade.c src/mydumper_chunks.c src/mydumper_write.c src/mydumper_arguments.c src/common_options.c src/mydumper_char_chunks.c src/mydumper_integer_chunks.c src/mydumper_partition_chunks.c ) #src/mydumper_multicolumn_integer_chunks.c)
SET( MYLOADER_SRCS src/myloader.c ${SHARED_SRCS} src/myloader_pmm_thread.c src/myloader_stream.c src/myloader_stream.c src/myloader_process.c src/myloader_common.c src/myloader_directory.c src/myloader_restore.c src/myloader_restore_job.c src/myloader_control_job.c src/myloader_intermediate_queue.c src/myloader_arguments.c src/common_options.c src/myloader_worker_index.c src/myloader_worker_schema.c src/myloader_worker_loader.c src/myloader_worker_post.c )

add_executable(mydumper ${MYDUMPER_SRCS})
add_executable(myloader ${MYLOADER_SRCS})

if (NOT JEMALLOC_FOUND)
  target_link_libraries(mydumper ${MYSQL_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES} ${GIO2_LIBRARIES} ${GOBJECT2_LIBRARIES} ${PCRE_PCRE_LIBRARY} ${ZLIB_LIBRARIES} stdc++ m )
  target_link_libraries(myloader ${MYSQL_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES} ${PCRE_PCRE_LIBRARY} ${ZLIB_LIBRARIES} stdc++)
else ()
  target_link_libraries(mydumper ${JEMALLOC_LIBRARIES} ${MYSQL_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES} ${GIO2_LIBRARIES} ${GOBJECT2_LIBRARIES} ${PCRE_PCRE_LIBRARY} ${ZLIB_LIBRARIES} stdc++ m )
  target_link_libraries(myloader ${JEMALLOC_LIBRARIES} ${MYSQL_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES} ${PCRE_PCRE_LIBRARY} ${ZLIB_LIBRARIES} stdc++)

endif ()

INSTALL(TARGETS mydumper myloader
  RUNTIME DESTINATION bin
)

INSTALL(FILES mydumper.cnf
  DESTINATION /etc
  COMPONENT config
)

add_custom_target(dist
  COMMAND git archive --format=tar.gz --prefix=${ARCHIVE_NAME}/
  -o ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}.tar.gz HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

OPTION(RUN_CPPCHECK "Run cppcheck" OFF)

IF(RUN_CPPCHECK)
  include(CppcheckTargets)
  add_cppcheck(mydumper)
  add_cppcheck(myloader)
ENDIF(RUN_CPPCHECK)


MESSAGE(STATUS "------------------------------------------------")
MESSAGE(STATUS "MYSQL_CONFIG = ${MYSQL_CONFIG}")
MESSAGE(STATUS "CMAKE_INSTALL_PREFIX = ${CMAKE_INSTALL_PREFIX}")
MESSAGE(STATUS "BUILD_DOCS = ${BUILD_DOCS}")
MESSAGE(STATUS "WITH_SSL = ${WITH_SSL}")
MESSAGE(STATUS "RUN_CPPCHECK = ${RUN_CPPCHECK}")
MESSAGE(STATUS "WITH_ASAN = ${WITH_ASAN}")
MESSAGE(STATUS "WITH_TSAN = ${WITH_TSAN}")
MESSAGE(STATUS "Change a values with: cmake -D<Variable>=<Value>")
MESSAGE(STATUS "------------------------------------------------")
MESSAGE(STATUS)


#get_cmake_property(_variableNames VARIABLES)
#list (SORT _variableNames)
#foreach (_variableName ${_variableNames})
#    message(STATUS "${_variableName}=${${_variableName}}")
#endforeach()



