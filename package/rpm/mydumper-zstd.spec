Name:           mydumper
Summary:        mydumper and myloader MySQL backup tools
Version:        %{version}
Release:        %{release}
Group:          Applications/Databases
License:        GPL
Vendor:         Max Bubenick
URL:            https://github.com/mydumper/mydumper
Source:         mydumper-%{version}%{extra_suffix}.tar.gz
BuildArch:      x86_64
AutoReq:        no
Requires:       libzstd
%define _rpmfilename %{name}-%{version}-%{release}%{extra_suffix}.%{distro}.%{arch}.rpm

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
install -m 0755 mydumper ${RPM_BUILD_ROOT}%{_bindir}
install -m 0755 myloader ${RPM_BUILD_ROOT}%{_bindir}

%clean
rm -rf ${RPM_BUILD_ROOT}

%files
%defattr(-,root,root,-)
%{_bindir}/*

%changelog
