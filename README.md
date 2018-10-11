# What is mydumper? Why?

* Parallelism (hence, speed) and performance (avoids expensive character set conversion routines, efficient code overall)
* Easier to manage output (separate files for tables, dump metadata, etc, easy to view/parse data)
* Consistency - maintains snapshot across all threads, provides accurate master and slave log positions, etc
* Manageability - supports PCRE for specifying database and tables inclusions and exclusions

## What is changed in this fork from upstream mydumper?

This fork contains additional fixes to work better with TiDB.  See the [PingCAP documentation for more](https://pingcap.com/docs/tools/mydumper/).

## How to install mydumper/myloader?

The best way to install, is via TiDB Enterprise Tools:

```
# Download the tool package.
wget http://download.pingcap.org/tidb-enterprise-tools-latest-linux-amd64.tar.gz
wget http://download.pingcap.org/tidb-enterprise-tools-latest-linux-amd64.sha256

# Check the file integrity. If the result is OK, the file is correct.
sha256sum -c tidb-enterprise-tools-latest-linux-amd64.sha256

# Extract the package.
tar -xzf tidb-enterprise-tools-latest-linux-amd64.tar.gz
cd tidb-enterprise-tools-latest-linux-amd64
```

## How to build it?

Run:

```bash
cmake .
make
```

One needs to install development versions of required libaries (MySQL, GLib, ZLib, PCRE):
NOTE: you must use the correspondent mysql devel package.

* Ubuntu or Debian: apt-get install libglib2.0-dev libmysqlclient15-dev zlib1g-dev libpcre3-dev libssl-dev
* Fedora, RedHat and CentOS: yum install glib2-devel mysql-devel zlib-devel pcre-devel openssl-devel
* openSUSE: zypper install glib2-devel libmysqlclient-devel pcre-devel zlib-devel
* MacOSX: port install glib2 mysql5 pcre pkgconfig cmake
 (You may want to run 'port select mysql mysql5' afterwards)

One has to make sure, that pkg-config, mysql_config, pcre-config are all in $PATH

Binlog dump is disabled by default to compile with it you need to add -DWITH_BINLOG=ON to cmake options

To build against mysql libs < 5.7 you need to disable SSL adding -DWITH_SSL=OFF

## How does consistent snapshot work?

Support for a consistent snapshot in TiDB *differs* from MySQL.  Instead of using `FLUSH TABLES WITH READ LOCK`, a `tidb_snapshot` is set for all sessions to ensure that they export data from the same point in time.

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

Of course, regex functionality can be used to describe pretty much any list of tables.


