Name:           mydumper
Summary:        mydumper and myloader MySQL backup tools
Version:        %{version}
Release:        %{release}
Group:          Applications/Databases
License:        GPL
Vendor:         Max Bubenick
URL:            https://github.com/maxbube/mydumper
Source:         mydumper-%{version}.tar.gz
BuildArch:      x86_64
AutoReq:        no
%define _rpmfilename %{name}-%{version}-%{release}.%{distro}.%{arch}.rpm

%description
This package provides mydumper and myloader MySQL backup tools.

mydumper is a tool used for backing up MySQL database servers much
faster than the mysqldump tool distributed with MySQL.  It also has the
capability to retrieve the binary logs from the remote server at the same time
as the dump itself.  The advantages of mydumper are: parallelism,
easier to manage output, consistency, manageability.

myloader is a tool used for multi-threaded restoration of mydumper backups.

%prep
%setup -q

%build
%define debug_package %{nil}

%install
install -m 0755 -d ${RPM_BUILD_ROOT}%{_bindir}
install -m 0555 mydumper ${RPM_BUILD_ROOT}%{_bindir}
install -m 0555 myloader ${RPM_BUILD_ROOT}%{_bindir}

%clean
rm -rf ${RPM_BUILD_ROOT}

%files
%defattr(-,root,root,-)
%{_bindir}/*

%changelog

