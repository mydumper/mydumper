# - Try to find the GLIB2 libraries

if(GLIB2_INCLUDE_DIRS AND GLIB2_LIBRARIES AND GTHREAD2_LIBRARIES)
    # Already in cache, be silent
    set(GLIB2_FIND_QUIETLY TRUE)
endif(GLIB2_INCLUDE_DIRS AND GLIB2_LIBRARIES AND GTHREAD2_LIBRARIES)

if (NOT WIN32)
   include(FindPkgConfig)
   pkg_search_module(GLIB2 REQUIRED glib-2.0)
   pkg_search_module(GTHREAD2 REQUIRED gthread-2.0)
endif(NOT WIN32)

mark_as_advanced(GLIB2_INCLUDE_DIRS GLIB2_LIBRARIES GTHREAD2_LIBRARIES)

