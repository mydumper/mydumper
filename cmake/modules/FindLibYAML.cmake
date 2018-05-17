# CMake module to search for the libyaml library
# (library for parsing YAML files)
# If it's found it sets LIBYAML_FOUND to TRUE
# and following variables are set:
#    LIBYAML_INCLUDE_DIR
#    LIBYAML_LIBRARY

find_package(PkgConfig)
pkg_check_modules(PC_LIBYAML yaml-0.1)

find_path(LIBYAML_INCLUDE_DIR NAMES yaml.h HINTS ${PC_LIBYAML_INCLUDEDIR}
	${PC_LIBYAML_INCLUDE_DIRS})
find_library(LIBYAML_LIBRARIES NAMES yaml libyaml
	HINTS ${PC_LIBYAML_LIBDIR} ${PC_LIBYAML_LIBRARY_DIR})

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LibYAML DEFAULT_MSG LIBYAML_LIBRARIES LIBYAML_INCLUDE_DIR)
MARK_AS_ADVANCED(LIBYAML_INCLUDE_DIR LIBYAML_LIBRARIES)
