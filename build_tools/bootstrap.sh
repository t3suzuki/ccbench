# prepare for masstree
cd third_party/masstree
./bootstrap.sh
./configure --disable-assertions
make clean
make -j CXXFLAGS='-g -W -Wall -O3 -fPIC -fcoroutines-ts -stdlib=libc++' libjson.a misc.o
ar cr libkohler_masstree_json.a json.o string.o straccum.o str.o msgpack.o clp.o kvrandom.o compiler.o memdebug.o kvthread.o misc.o
ranlib libkohler_masstree_json.a

