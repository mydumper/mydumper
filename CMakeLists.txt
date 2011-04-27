cmake_minimum_required(VERSION 2.6)
project(mydumper)
set(VERSION 0.2.0)
set(ARCHIVE_NAME "${CMAKE_PROJECT_NAME}-${VERSION}")

#Required packages
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/modules)
find_package(MySQL)
find_package(ZLIB)
find_package(GLIB2)
find_package(PCRE)

add_subdirectory(docs)

set(CMAKE_C_FLAGS "-Wall -Wunused -Wwrite-strings -Wno-strict-aliasing -Wextra -Werror -Wshadow -O3 -g")

include_directories(${MYDUMPER_SOURCE_DIR} ${MYSQL_INCLUDE_DIR} ${GLIB2_INCLUDE_DIR} ${PCRE_INCLUDE_DIR} ${ZLIB_INCLUDE_DIRS})

if (NOT CMAKE_INSTALL_PREFIX)
  SET(CMAKE_INSTALL_PREFIX "/usr/local" CACHE STRING "Install path" FORCE)
endif (NOT CMAKE_INSTALL_PREFIX)
MARK_AS_ADVANCED(CMAKE)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${CMAKE_CURRENT_BINARY_DIR}/config.h)

add_executable(mydumper mydumper.c binlog.c)
target_link_libraries(mydumper ${MYSQL_LIBRARY} ${MYSQL_EXTRA_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES} ${PCRE_PCRE_LIBRARY} ${ZLIB_LIBRARIES})

INSTALL(TARGETS mydumper
  RUNTIME DESTINATION bin
)

add_custom_target(dist
  COMMAND bzr export --root=${ARCHIVE_NAME}
    ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}.tar.gz
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
