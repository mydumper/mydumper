#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#        Authors:        David Ducos, Percona (david dot ducos at percona dot com)

if(ZSTD_INCLUDE_DIR AND ZSTD_LIBRARIES)
    # Already in cache, be silent
    set(ZSTD_FIND_QUIETLY TRUE)
endif(ZSTD_INCLUDE_DIR AND ZSTD_LIBRARIES)

if (NOT WIN32)
   include(FindPkgConfig)
   pkg_search_module(PC_ZSTD REQUIRED libzstd)
endif(NOT WIN32)

set(ZSTD_INCLUDE_DIR ${PC_ZSTD_INCLUDE_DIRS})

find_library(ZSTD_LIBRARIES NAMES zstd HINTS ${PC_ZSTD_LIBDIR} ${PC_ZSTD_LIBRARY_DIRS})

mark_as_advanced(ZSTD_INCLUDE_DIR ZSTD_LIBRARIES)

