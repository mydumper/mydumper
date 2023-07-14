#!/bin/sh
#
# This script builds RPM and DEB package for mydumper.
# To compile binaries look at https://github.com/mydumper/mydumper_builder
# Requirements: yum install rpm-build dpkg dpkg-devel fakeroot

SOURCE=/tmp/src/mydumper
TARGET=/tmp/package/
WORK_DIR=/tmp/pkgbuild-`date +%s`
set -e

PROJECT=mydumper
WORKSPACE=$(dirname "$(readlink -f "$0")")

if [ "$#" = 5 ]; then
    VERSION=$1
    RELEASE=$2
    KIND=$3
    DIR=$4
    ARCH=$5
else
    echo "USAGE: sh build.sh <version> <revision> [rpm|deb] <directory> <architecture>"
    exit 1
fi

build_rpm() {
    SUBDIR=$1
    DISTRO=$2

    mkdir -p $WORK_DIR/{BUILD,BUILDROOT,RPMS,SOURCES,SRPMS} $WORK_DIR/SOURCES/$PROJECT-$VERSION $TARGET
    ls $SOURCE/$SUBDIR/*
    cp -r $SOURCE/$SUBDIR/* $WORK_DIR/SOURCES/$PROJECT-$VERSION
    cd $WORK_DIR/SOURCES
    tar czf $PROJECT-$VERSION.tar.gz $PROJECT-$VERSION/
    cd ..
    rpmbuild -ba $WORKSPACE/rpm/${PROJECT}.spec \
             --define "_topdir $WORK_DIR" \
             --define "version $VERSION" \
             --define "release $RELEASE" \
             --define "distro $DISTRO"
    PKG=$PROJECT-$VERSION-${RELEASE}.$DISTRO.$ARCH.rpm
    mv RPMS/$PKG $TARGET

    rpm -qpil --requires $TARGET/$PKG
    echo
    echo "RPM done: $TARGET/$PKG"
    echo
    rm -rf $WORK_DIR
}

build_deb() {
    SUBDIR=$1
    DISTRO=$2
    mkdir -p $WORK_DIR/${PROJECT}_${VERSION}/DEBIAN $TARGET
    cd $WORK_DIR
    cp $WORKSPACE/deb/copyright $WORKSPACE/deb/files $WORKSPACE/deb/rules $WORK_DIR/${PROJECT}_$VERSION/DEBIAN/
    cp $WORKSPACE/deb/control $WORK_DIR/${PROJECT}_$VERSION/DEBIAN/

    sed -i "s/%{version}/$VERSION-$RELEASE/" $WORK_DIR/${PROJECT}_$VERSION/DEBIAN/control
    sed -i "s/%{distro}/$DISTRO/" $WORK_DIR/${PROJECT}_$VERSION/DEBIAN/control
    $WORKSPACE/deb/files $SOURCE/$SUBDIR $WORK_DIR/${PROJECT}_$VERSION

    fakeroot dpkg-deb -Zxz --build ${PROJECT}_$VERSION
    PKG=${PROJECT}_$VERSION-${RELEASE}~${DISTRO}_${ARCH}.deb
    mv ${PROJECT}_$VERSION.deb $TARGET/$PKG

    echo
    dpkg -I $TARGET/$PKG
    dpkg -c $TARGET/$PKG
    echo
    echo "DEB done: $TARGET/$PKG"
    echo
#    rm -rf $WORK_DIR
}

if [ "$KIND" = "rpm" ]
then
        build_rpm $DIR $(echo $DIR | cut -d'_' -f1)
fi

if [ "$KIND" = "deb" ]
then
        build_deb $DIR $(echo $DIR | cut -d'_' -f1)
fi
