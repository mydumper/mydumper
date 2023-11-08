Name:           mydumper
Summary:        mydumper and myloader MySQL backup tools
Version:        %{version}
Release:        %{release}
Group:          Applications/Databases
License:        GPL
Vendor:         David Ducos
URL:            https://github.com/mydumper/mydumper
Source:         mydumper-%{version}.tar.gz
BuildArch:      %{architecture}
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
install -m 0755 -d ${RPM_BUILD_ROOT}%{_sysconfdir}
install -m 0755 mydumper ${RPM_BUILD_ROOT}%{_bindir}
install -m 0755 myloader ${RPM_BUILD_ROOT}%{_bindir}
install -m 0664 mydumper.cnf ${RPM_BUILD_ROOT}%{_sysconfdir}
%define install_include %(test -f SOURCES/install.inc && echo $_)
%if "%{install_include}"
    %include %{install_include}
%endif

%clean
rm -rf ${RPM_BUILD_ROOT}

%files
%defattr(-,root,root,-)
%config(noreplace) %{_sysconfdir}/*
%{_bindir}/*
%define files_include %(test -f SOURCES/files.inc && echo $_)
%if "%{files_include}"
    %include %{files_include}
%endif

%changelog
