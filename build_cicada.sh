#!/bin/sh

cicada-engine/script/setup.sh 0 0

cd cicada-engine || exit 1

mkdir build
cd build

cmake ..
make -j8

ln -s ../src/mica/test/test_tx.json ./

