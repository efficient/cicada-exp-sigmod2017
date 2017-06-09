#!/bin/sh

../script/setup.sh 0 0

cd silo || exit 1

MODE=perf DEBUG=0 CHECK_INVARIANTS=0 make -j8 dbtest
