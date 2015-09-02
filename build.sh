#!/bin/bash

set -e

mkdir -p build
cd build

cmake .. -DWITH_MAXSCALE_CNF=N -DDEBUG=1 -DTHREADED=Y -DSTATIC_EMBEDDED=false -DCMAKE_INSTALL_PREFIX=~sdavidovitz/maxscale -DWITH_SCRIPTS=N -DBUILD_TESTS=Y -DMAXSCALE_VARDIR=~sdavidovitz/maxscale/var -DCMAKE_BUILD_TYPE=Debug
make
