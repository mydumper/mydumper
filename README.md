[![CircleCI](https://circleci.com/gh/mydumper/mydumper/tree/master.svg?style=svg)](https://circleci.com/gh/mydumper/mydumper/tree/master)
 <span class="badge-geekswag">
<a href="https://geekswag.co/collections/mydumper" title="Get Our Merch"><img src="https://img.shields.io/badge/Geekswag-Get%20Our%20Merch-brightgreen" alt="Get a Merch donate button" /></a>

 # What is MyDumper? 
MyDumper is a Logical Backup Tool. It has 2 tools: 
* mydumper which is responsible to export a consistent backup 
* myloader reads the backup from mydumper, connects the to destination database and imports the backup.
Both tools use multithreading capabilities

# Why do we need MyDumper?
* Parallelism (hence, speed) and performance (avoids expensive character set conversion routines, efficient code overall)
* Easier to manage output (separate files for tables, dump metadata, etc, easy to view/parse data)
* Consistency - maintains snapshot across all threads, provides accurate master and slave log positions, etc
* Manageability - supports PCRE for specifying database and tables inclusions and exclusions

## How to install mydumper/myloader?

First get the correct url from the [releases section](https://github.com/maxbube/mydumper/releases) then:

### RedHat / Centos

```bash
yum install https://github.com/mydumper/mydumper/releases/download/v0.11.3-2/mydumper-0.11.3-2.el7.x86_64.rpm
yum install https://github.com/mydumper/mydumper/releases/download/v0.11.3-2/mydumper-0.11.3-2.el8.x86_64.rpm
```

### Ubuntu / Debian
For ubuntu, you need to install the dependencies:
```bash
apt-get install libatomic1
```
Then you can download and install the package:
```bash
wget https://github.com/mydumper/mydumper/releases/download/v0.11.3-2/mydumper_0.11.3-2.$(lsb_release -cs)_amd64.deb
dpkg -i mydumper_0.11.3-2.$(lsb_release -cs)_amd64.deb
```

### FreeBSD
By using pkg

```bash
pkg install mydumper
```
or from ports

```bash
cd /usr/ports/databases/mydumper && make install
```

### OSX
By using [Homebrew](https://brew.sh/)

```bash
brew install mydumper
```

## Dependencies for building mydumper

### One needs to install development tools:
* Ubuntu or Debian: 
```
apt-get install cmake g++ git
```
* Fedora, RedHat and CentOS:
```
yum install -y cmake gcc gcc-c++ git make
```
* MacOSX:

```
brew install cmake pkg-config sphinx-doc glib mysql-client openssl@1.1 pcre
```

```
port install pkgconfig cmake
```
### One needs to install development versions of GLib, ZLib, PCRE and ZSTD:
* Ubuntu or Debian: 
```
apt-get install libglib2.0-dev zlib1g-dev libpcre3-dev libssl-dev libzstd-dev
```
* Fedora, RedHat and CentOS: 
```
yum install -y glib2-devel mysql-devel openssl-devel pcre-devel zlib-devel libzstd-devel
```
* openSUSE: 
```
zypper install glib2-devel libmysqlclient-devel pcre-devel zlib-devel
```
* MacOSX: 
```
port install glib2 pcre 
```
### One needs to install MySQL/Percona/MariaDB development versions:
* Ubuntu or Debian: 
```
apt-get install libmysqlclient-dev
apt-get install libperconaserverclient20-dev
apt-get install libmariadbclient-dev 
```
* Fedora, RedHat and CentOS: 
```
yum install -y mysql-devel
yum install -y Percona-Server-devel-57
yum install -y mariadb-devel
```
CentOS 7 comes by default with MariaDB 5.5 libraries which are very old.
  It might be better to download a newer version of these libraries (MariaDB, MySQL, Percona etc).
* openSUSE: 
```
zypper install libmysqlclient-devel
```
* MacOSX: port install mysql5
 (You may want to run 'port select mysql mysql5' afterwards)

## How to build it?

Run:

```bash
cmake .
make
```

One has to make sure, that pkg-config, mysql_config, pcre-config are all in $PATH

Binlog dump is disabled by default to compile with it you need to add -DWITH_BINLOG=ON to cmake options

To build against mysql libs < 5.7 you need to disable SSL adding -DWITH_SSL=OFF

# How to use mydumper

See [Usage](docs/mydumper_usage.rst)

## How does consistent snapshot work?

This is all done following best MySQL practices and traditions:

* As a precaution, slow running queries on the server either abort the dump, or get killed
* Global read lock is acquired ("FLUSH TABLES WITH READ LOCK")
* Various metadata is read ("SHOW SLAVE STATUS","SHOW MASTER STATUS")
* Other threads connect and establish snapshots ("START TRANSACTION WITH CONSISTENT SNAPSHOT")
** On pre-4.1.8 it creates dummy InnoDB table, and reads from it.
* Once all worker threads announce the snapshot establishment, master executes "UNLOCK TABLES" and starts queueing jobs.

This for now does not provide consistent snapshots for non-transactional engines - support for that is expected in 0.2 :)

## How to exclude (or include) databases?

Once can use --regex functionality, for example not to dump mysql and test databases:

```bash
 mydumper --regex '^(?!(mysql\.|test\.))'
```

To dump only mysql and test databases:

```bash
 mydumper --regex '^(mysql\.|test\.)'
```

To not dump all databases starting with test:

```bash
 mydumper --regex '^(?!(test))'
```

To dump specify tables in different databases (Note: The name of tables should end with $. [related issue](https://github.com/maxbube/mydumper/issues/407)):

```bash
 mydumper --regex '^(db1\.table1$|db2\.table2$)'
```

If you want to dump a couple of databases but discard some tables, you can do:
```bash
 mydumper --regex '^(?=(?:(db1\.|db2\.)))(?!(?:(db1\.table1$|db2\.table2$)))'
```
Which will dump all the tables in db1 and db2 but it will exclude db1.table1 and db2.table2

Of course, regex functionality can be used to describe pretty much any list of tables.
