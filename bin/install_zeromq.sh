#!/bin/bash
WS_ROOT=`dirname $0`/..
echo $WS_ROOT
WS_BUILD=$WS_ROOT/build
WS_LIB=$WS_ROOT/lib

mkdir -p $WS_BUILD
mkdir -p $WS_LIB
cd $WS_BUILD

git clone https://github.com/zeromq/jzmq.git
cd -
cd $WS_BUILD/jzmq
./autogen.sh
./configure
make
sudo make install
cd -
cp $WS_BUILD/jzmq/src/zmq.jar $WS_LIB
