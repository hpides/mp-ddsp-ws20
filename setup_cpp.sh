#!/bin/sh
# GCC 10
echo | sudo add-apt-repository ppa:ubuntu-toolchain-r/test &&
sudo apt-get install gcc-10 g++-10 &&
export CC=/usr/local/bin/gcc-10 &&
export CXX=/usr/local/bin/g++-10;

# Clang Format
sudo apt install clang-format;

# CMake
mkdir cmake;
wget https://github.com/Kitware/CMake/releases/download/v3.19.4/cmake-3.19.4.tar.gz;
tar xf cmake-3.19.4.tar.gz;
chmod 700 bootstrap;
./bootstrap --parallel=4 --prefix=../cmake -- -DCMAKE_BUILD_TYPE:STRING=Release -DCMAKE_USE_OPENSSL=OFF;
make -j 4; make install -j 4;

# MPI
mkdir openmpi;
wget https://download.open-mpi.org/release/open-mpi/v4.1/openmpi-4.1.0.tar.gz;
tar xf openmpi-4.1.0.tar.gz;
cd openmpi-4.1.0/;
./configure --prefix $(pwd)/../openmpi --enable-mpi-thread-multiple --enable-orterun-prefix-by-default --without-verbs;
make all -j 4;
make install -j 4;
cd ..;
ln -s generator/openmpi openmpi;
ln -s hardcoded_streaming_engine/openmpi openmpi;

# self-built openmpi on 16 node cluster:
# before running configure, do:
# (not 100% sure, if this is correct, but something like that)
mkdir ~/numa && cd numa;
ln -s /usr/lib/x86_64-linux-gnu/libnuma.so.1 libnuma.so;
export LDFLAGS=-L/hpi/fs00/home/$(whoami)/numa/;

# pre-built openmpi on 16 node cluster:
# mpi Path is: /usr/mpi/gcc/openmpi-4.1.0rc5/
# just set the symbolic link for openmpi to this directory in the according project directories
