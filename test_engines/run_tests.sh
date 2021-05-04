#!/bin/bash

tested_engine=$1;
echo testing engine $tested_engine;

cd ../generator; mkdir build; cd build; cmake -DCMAKE_BUILD_TYPE=Release .. > /dev/null; make -j 4 > /dev/null;
cd ../../;

cd $tested_engine; mkdir build; cd build; cmake -DCMAKE_BUILD_TYPE=Release .. > /dev/null; make -j 4 > /dev/null;
if [ "$tested_engine" = "string_engine" ]; 
then 
    ./$tested_engine;
fi
cd ../../;


echo start generator;
(./generator/build/engine_test --count 26 --portAd 12345 --portCheckout 12346 --sleepTime 1000 --dataGeneratedAfterEachSleep 2 --bufferThreshold 26 --adSourcePath "test_engines/adSource.csv" --checkoutSourcePath "test_engines/checkoutSource.csv" &) | grep -q "Port"

echo start streaming engine;
if [ "$tested_engine" = "string_engine" ]; 
then
    timeout 60s ./$tested_engine/cmake-build-debug/compiled_queries/query > test_engines/results/$tested_engine.csv;
else
    timeout 60s ./$tested_engine/build/$tested_engine > test_engines/results/$tested_engine.csv;
fi

cd test_engines;
python3 evaluate_results.py results/$tested_engine.csv;

exit $?;
