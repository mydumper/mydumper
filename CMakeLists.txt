cmake_minimum_required(VERSION 2.6)
project(mydumper)
set(VERSION 0.12.7-3)
set(ARCHIVE_NAME "${CMAKE_PROJECT_NAME}-${VERSION}")

#Required packages
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/modules)
find_package(MySQL)
find_package(ZLIB)
find_package(GLIB2)
find_package(PCRE)
find_package(OpenSSL)

get_cmake_property(_variableNames VARIABLES)
list (SORT _variableNames)
foreach (_variableName ${_variableNames})
  string( TOLOWER "${_variableName}" var_lower )
  if( var_lower STREQUAL "openssl_found" )
    set(MYDUMPER_OPENSSL_FOUND "${${_variableName}}")
  endif (var_lower STREQUAL "openssl_found")
endforeach()

if (NOT (MYDUMPER_OPENSSL_FOUND))
  option(WITH_SSL FALSE)
  string(REPLACE "MYSQL_LIBRARIES_ssl-NOTFOUND"    "" MYSQL_LIBRARIES "${MYSQL_LIBRARIES}")
  string(REPLACE "MYSQL_LIBRARIES_crypto-NOTFOUND" "" MYSQL_LIBRARIES "${MYSQL_LIBRARIES}")
endif (NOT (MYDUMPER_OPENSSL_FOUND))

option(BUILD_DOCS "Build the documentation" ON)

if (BUILD_DOCS)
  add_subdirectory(docs)
endif (BUILD_DOCS)

if (PC_GLIB2_VERSION VERSION_GREATER "2.51")
  option(WITH_GLIB_uuid_string_random "uuid_string_random not available" ON)
endif (PC_GLIB2_VERSION VERSION_GREATER "2.51")

option(WITH_SSL "Build SSL support" ON)
option(WITH_ZSTD "Build ZSTD support" OFF)
if (WITH_ZSTD)
  find_package(ZSTD)
endif (WITH_ZSTD)

if (WITH_ZSTD)
  set(CMAKE_C_FLAGS "-Wall -Wno-deprecated-declarations -Wunused -Wwrite-strings -Wno-strict-aliasing -Wextra -Wshadow -O3 -g -DZWRAP_USE_ZSTD=1 -Werror -Wno-discarded-qualifiers ${MYSQL_CFLAGS}")
  include_directories(${MYDUMPER_SOURCE_DIR} ${MYSQL_INCLUDE_DIR} ${GLIB2_INCLUDE_DIR} ${PCRE_INCLUDE_DIR} ${ZLIB_INCLUDE_DIRS} ${ZSTD_INCLUDE_DIR} )
else (WITH_ZSTD)  
  set(CMAKE_C_FLAGS "-Wall -Wno-deprecated-declarations -Wunused -Wwrite-strings -Wno-strict-aliasing -Wextra -Wshadow -O3 -g -Werror ${MYSQL_CFLAGS}")
  include_directories(${MYDUMPER_SOURCE_DIR} ${MYSQL_INCLUDE_DIR} ${GLIB2_INCLUDE_DIR} ${PCRE_INCLUDE_DIR} ${ZLIB_INCLUDE_DIRS} )
endif (WITH_ZSTD)

if (NOT CMAKE_INSTALL_PREFIX)
  SET(CMAKE_INSTALL_PREFIX "/usr/local" CACHE STRING "Install path" FORCE)
endif (NOT CMAKE_INSTALL_PREFIX)
MARK_AS_ADVANCED(CMAKE)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_SOURCE_DIR}/src/config.h )
SET( SHARED_SRCS src/server_detect.c src/connection.c src/logging.c src/set_verbose.c src/common.c src/tables_skiplist.c src/regex.c )
SET( ZSTD_SRCS zstd/zstd_zlibwrapper.c zstd/gzclose.c zstd/gzlib.c zstd/gzread.c zstd/gzwrite.c )
SET( MYDUMPER_SRCS src/mydumper.c ${SHARED_SRCS} src/mydumper_pmm_thread.c src/mydumper_start_dump.c src/mydumper_jobs.c src/mydumper_common.c src/mydumper_stream.c src/mydumper_database.c src/mydumper_working_thread.c src/mydumper_daemon_thread.c src/mydumper_exec_command.c src/mydumper_masquerade.c )
SET( MYLOADER_SRCS src/myloader.c ${SHARED_SRCS} src/myloader_pmm_thread.c src/myloader_stream.c src/myloader_stream.c src/myloader_process.c src/myloader_common.c src/myloader_jobs_manager.c src/myloader_directory.c src/myloader_restore.c src/myloader_restore_job.c src/myloader_control_job.c src/myloader_intermediate_queue.c )

if (WITH_ZSTD)
  add_executable(mydumper ${MYDUMPER_SRCS} ${ZSTD_SRCS})
  target_link_libraries(mydumper ${MYSQL_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES} ${GIO2_LIBRARIES} ${GOBJECT2_LIBRARIES} ${PCRE_PCRE_LIBRARY} ${ZLIB_LIBRARIES} ${ZSTD_LIBRARIES} stdc++ m )

  add_executable(myloader ${MYLOADER_SRCS} ${ZSTD_SRCS})
  target_link_libraries(myloader ${MYSQL_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES} ${PCRE_PCRE_LIBRARY} ${ZLIB_LIBRARIES} ${ZSTD_LIBRARIES} stdc++)

else (WITH_ZSTD)
  add_executable(mydumper ${MYDUMPER_SRCS})
  target_link_libraries(mydumper ${MYSQL_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES} ${GIO2_LIBRARIES} ${GOBJECT2_LIBRARIES} ${PCRE_PCRE_LIBRARY} ${ZLIB_LIBRARIES} stdc++ m )

  add_executable(myloader ${MYLOADER_SRCS})
  target_link_libraries(myloader ${MYSQL_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES} ${PCRE_PCRE_LIBRARY} ${ZLIB_LIBRARIES} stdc++)
endif (WITH_ZSTD)



INSTALL(TARGETS mydumper myloader
  RUNTIME DESTINATION bin
)

add_custom_target(dist
  COMMAND bzr export --root=${ARCHIVE_NAME}
    ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}.tar.gz
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
MESSAGE(STATUS "WITH_ZSTD = ${WITH_ZSTD}")
MESSAGE(STATUS "OpenSSL_FOUND = ${MYDUMPER_OPENSSL_FOUND}")
MESSAGE(STATUS "WITH_SSL = ${WITH_SSL}")
MESSAGE(STATUS "RUN_CPPCHECK = ${RUN_CPPCHECK}")
MESSAGE(STATUS "Change a values with: cmake -D<Variable>=<Value>")
MESSAGE(STATUS "------------------------------------------------")
MESSAGE(STATUS)

