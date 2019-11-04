#!/bin/bash

mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j20 c_bench
