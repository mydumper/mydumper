Compiling
=========

Requirements
------------

mydumper requires the following before it can be compiled:

 * `CMake <http://www.cmake.org/>`_
 * `Glib2 <http://www.gtk.org/index.php>`_ (with development packages)
 * `PCRE <http://www.pcre.org/>`_ (with development packages)
 * `MySQL <http://www.mysql.com/>`_ client libraries (with development packages)

Additionally the following packages are optional:

 * `python-sphinx <http://sphinx.pocoo.org/>`_ (for documentation)

Ubuntu/Debian
^^^^^^^^^^^^^

.. code-block::  bash

   apt-get install libglib2.0-dev libmysqlclient15-dev zlib1g-dev libpcre3-dev

Fedora/Redhat/CentOS
^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   yum install glib2-devel mysql-devel zlib-devel pcre-devel

OpenSUSE
^^^^^^^^

.. code-block:: bash

   zypper install glib2-devel libmysqlclient-devel pcre-devel zlib-devel

Mac OSX
^^^^^^^

.. code-block:: bash

   port install glib2 mysql5 pcre

CMake
-----

CMake is used for mydumper's build system and is executed as follows::

  cmake .
  make

You can optionally provide parameters for CMake, the possible options are:

 * ``-DMYSQL_CONFIG=/path/to/mysql_config`` - The path and filename for the mysql_config executable
 * ``-DCMAKE_INSTALL_PREFIX=/install/path`` - The path where mydumper should be installed

Documentation
-------------

If you wish to just compile the documentation you can do so with::

  cmake .
  make doc_html

or for a man page output::

  cmake .
  make doc_man
