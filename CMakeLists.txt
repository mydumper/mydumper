cmake_minimum_required(VERSION 2.6)
project(mydumper)
set(VERSION 0.11.2)
set(ARCHIVE_NAME "${CMAKE_PROJECT_NAME}-${VERSION}")

#Required packages
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/modules)
find_package(MySQL)
find_package(ZLIB)
find_package(GLIB2)
find_package(PCRE)

option(BUILD_DOCS "Build the documentation" ON)

if (BUILD_DOCS)
  add_subdirectory(docs)
endif (BUILD_DOCS)

option(WITH_BINLOG "Build binlog dump options" OFF)
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

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_SOURCE_DIR}/config.h)

if (WITH_ZSTD)
if (WITH_BINLOG)
  add_executable(mydumper mydumper.c binlog.c server_detect.c g_unix_signal.c connection.c getPassword.c logging.c set_verbose.c common.c zstd/zstd_zlibwrapper.c zstd/gzclose.c zstd/gzlib.c zstd/gzread.c zstd/gzwrite.c ) 
else (WITH_BINLOG)
  add_executable(mydumper mydumper.c server_detect.c g_unix_signal.c connection.c getPassword.c logging.c set_verbose.c common.c zstd/zstd_zlibwrapper.c zstd/gzclose.c zstd/gzlib.c zstd/gzread.c zstd/gzwrite.c)
endif (WITH_BINLOG)
target_link_libraries(mydumper ${MYSQL_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES} ${GIO2_LIBRARIES} ${GOBJECT2_LIBRARIES} ${PCRE_PCRE_LIBRARY} ${ZLIB_LIBRARIES} ${ZSTD_LIBRARIES} stdc++ m )

add_executable(myloader myloader.c connection.c getPassword.c logging.c set_verbose.c common.c zstd/zstd_zlibwrapper.c zstd/gzclose.c zstd/gzlib.c zstd/gzread.c zstd/gzwrite.c)
target_link_libraries(myloader ${MYSQL_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES} ${PCRE_PCRE_LIBRARY} ${ZLIB_LIBRARIES} ${ZSTD_LIBRARIES} stdc++)

else (WITH_ZSTD)
if (WITH_BINLOG)
  add_executable(mydumper mydumper.c binlog.c server_detect.c g_unix_signal.c connection.c getPassword.c logging.c set_verbose.c common.c)
else (WITH_BINLOG)
  add_executable(mydumper mydumper.c server_detect.c g_unix_signal.c connection.c getPassword.c logging.c set_verbose.c common.c)
endif (WITH_BINLOG)
target_link_libraries(mydumper ${MYSQL_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES} ${GIO2_LIBRARIES} ${GOBJECT2_LIBRARIES} ${PCRE_PCRE_LIBRARY} ${ZLIB_LIBRARIES} stdc++ m )

add_executable(myloader myloader.c connection.c getPassword.c logging.c set_verbose.c common.c)
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
MESSAGE(STATUS "WITH_BINLOG = ${WITH_BINLOG}")
MESSAGE(STATUS "WITH_ZSTD = ${WITH_ZSTD}")
MESSAGE(STATUS "RUN_CPPCHECK = ${RUN_CPPCHECK}")
MESSAGE(STATUS "Change a values with: cmake -D<Variable>=<Value>")
MESSAGE(STATUS "------------------------------------------------")
MESSAGE(STATUS)

