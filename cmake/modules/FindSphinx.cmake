# - This module looks for Sphinx
# Find the Sphinx documentation generator
#
# This modules defines
#  SPHINX_EXECUTABLE
#  SPHINX_FOUND
#  SPHINX_MAJOR_VERSION
#  SPHINX_MINOR_VERSION
#  SPHINX_VERSION

#=============================================================================
# Copyright 2002-2009 Kitware, Inc.
# Copyright 2009-2011 Peter Colberg
#
# Distributed under the OSI-approved BSD License (the "License");
# see accompanying file COPYING-CMAKE-SCRIPTS for details.
#
# This software is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the License for more information.
#=============================================================================
# (To distribute this file outside of CMake, substitute the full
#  License text for the above reference.)

find_program(SPHINX_EXECUTABLE NAMES sphinx-build
  HINTS
  $ENV{SPHINX_DIR}
  PATH_SUFFIXES bin
  DOC "Sphinx documentation generator"
)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(Sphinx DEFAULT_MSG
  SPHINX_EXECUTABLE
)

if (SPHINX_EXECUTABLE)
  execute_process (
    COMMAND "${SPHINX_EXECUTABLE}" -h
    OUTPUT_VARIABLE _SPHINX_VERSION_OUTPUT
    ERROR_VARIABLE _SPHINX_VERSION_OUTPUT
  )
  if (_SPHINX_VERSION_OUTPUT MATCHES "Sphinx v([0-9]+\\.[0-9]+\\.[0-9]+)")
    set (SPHINX_VERSION "${CMAKE_MATCH_1}")
    string (REPLACE "." ";" _SPHINX_VERSION_LIST "${SPHINX_VERSION}")
    list (GET _SPHINX_VERSION_LIST 0 SPHINX_MAJOR_VERSION)
    list (GET _SPHINX_VERSION_LIST 1 SPHINX_MINOR_VERSION)
    # patch version meh :)
  endif()
endif()

message("${SPHINX_MAJOR_VERSION}")

mark_as_advanced(
  SPHINX_EXECUTABLE
)
