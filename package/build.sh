#!/bin/sh
#
# This script builds RPM and DEB package for mydumper.
# To compile binaries look at https://github.com/maxbube/mydumper_builder
# Requirements: yum install rpm-build dpkg dpkg-devel fakeroot

SOURCE=/opt/src/mydumper
TARGET=/opt/PKGS
WORK_DIR=/tmp/pkgbuild-`date +%s`
YUM_REPO=/usr/share/nginx/html/rpm-contrib
APT_REPO=/usr/share/nginx/html/deb-contrib

set -e

PROJECT=mydumper
WORKSPACE=$(dirname "$(readlink -f "$0")")

if [ "$#" = 2 ]; then
    VERSION=$1
    RELEASE=$2
else
    echo "USAGE: sh build.sh <version> <revision>"
    exit 1
fi

build_rpm() {
    ARCH=x86_64
    SUBDIR=$1
    DISTRO=$2

    mkdir -p $WORK_DIR/{BUILD,BUILDROOT,RPMS,SOURCES,SRPMS} $WORK_DIR/SOURCES/$PROJECT-$VERSION $TARGET
    cp -r $SOURCE/$SUBDIR/* $WORK_DIR/SOURCES/$PROJECT-$VERSION
    cd $WORK_DIR/SOURCES
    tar czf $PROJECT-$VERSION.tar.gz $PROJECT-$VERSION/
    cd ..

    rpmbuild -ba $WORKSPACE/rpm/$PROJECT.spec \
             --define "_topdir $WORK_DIR" \
             --define "version $VERSION" \
             --define "release $RELEASE" \
             --define "distro $DISTRO"
    PKG=$PROJECT-$VERSION-$RELEASE.$DISTRO.$ARCH.rpm
    mv RPMS/$PKG $TARGET

    rpm -qpil --requires $TARGET/$PKG
    echo
    echo "RPM done: $TARGET/$PKG"
    echo
    rm -rf $WORK_DIR
}

build_deb() {
    ARCH=amd64
    SUBDIR=$1
    DISTRO=$2

    mkdir -p $WORK_DIR/${PROJECT}_$VERSION/DEBIAN $TARGET
    cd $WORK_DIR
    cp $WORKSPACE/deb/* $WORK_DIR/${PROJECT}_$VERSION/DEBIAN/
    sed -i "s/%{version}/$VERSION-$RELEASE/" $WORK_DIR/${PROJECT}_$VERSION/DEBIAN/control
    $WORKSPACE/deb/files $SOURCE/$SUBDIR $WORK_DIR/${PROJECT}_$VERSION

    fakeroot dpkg --build ${PROJECT}_$VERSION
    PKG=${PROJECT}_$VERSION-${RELEASE}.${DISTRO}_${ARCH}.deb
    mv ${PROJECT}_$VERSION.deb $TARGET/$PKG

    echo
    dpkg -I $TARGET/$PKG
    dpkg -c $TARGET/$PKG
    echo
    echo "DEB done: $TARGET/$PKG"
    echo
    rm -rf $WORK_DIR
}

# function "source dir" "distro"
build_rpm "el6" "el6"
build_rpm "el7" "el7"
build_deb "trusty" "trusty"
build_deb "xenial" "xenial"
build_deb "wheezy" "wheezy"
build_deb "jessie" "jessie"
build_deb "stretch" "stretch"
build_deb "bionic" "bionic"

# Building from Jenkins
if [ -n "${JOB_NAME}" ]; then
    cd $TARGET
    mv *.el6.x86_64.rpm $YUM_REPO/6
    mv *.el7.x86_64.rpm $YUM_REPO/7
    createrepo --update $YUM_REPO/6
    createrepo --update $YUM_REPO/7
    createrepo --update $YUM_REPO
    echo "YUM repo updated."
    echo
    for DISTRO in {trusty,xenial,wheezy,jessie,stretch,bionic}; do
        cd $TARGET
        mv *.${DISTRO}_amd64.deb $APT_REPO/$DISTRO/
        cd $APT_REPO/$DISTRO
        dpkg-scanpackages -m . | gzip -9c > Packages.gz; gunzip -c Packages.gz > Packages
    done
    echo "APT repo updated."
    echo
fi

exit 0
