#!/bin/bash

cd ../generator; rm -rf build/; mkdir build; cd build; cmake -DCMAKE_BUILD_TYPE=Release ..; make -j 4;
cd ../../;

cd streaming_engine; rm -rf build/; mkdir build; cd build; cmake -DCMAKE_BUILD_TYPE=Release ..; make -j 4;
cd ../../;

cd hardcoded_streaming_engine; rm -rf build/; mkdir build; cd build; cmake -DCMAKE_BUILD_TYPE=Release ..; make -j 4;
cd ../../;

cd string_engine; rm -rf build/; mkdir build; cd build; cmake -DCMAKE_BUILD_TYPE=Release ..; make -j 4;
cd ../../;

echo start generator;
(./generator/build/generator --recordCount $1 --portAd 12345 --portCheckout 12346 &) | grep -q "Port"

echo start streaming engine;
./streaming_engine/build/streaming_engine > visualize_benchmark/operator_engine.csv;

sleep 2;
echo ======;

echo start generator;
(./generator/build/generator --recordCount $1 --portAd 12345 --portCheckout 12346 &) | grep -q "Port"

echo start harcoded streaming engine;
./hardcoded_streaming_engine/build/hardcoded_streaming_engine > visualize_benchmark/hardcoded.csv;

sleep 2;
echo ======;

echo building string pipeline;
cd string_engine/build; ./string_engine; cd ../..;

echo start generator;
(./generator/build/generator --recordCount $1 --portAd 12345 --portCheckout 12346 &) | grep -q "Port"

echo start compiled query;
./string_engine/build/compiled_queries/query > visualize_benchmark/string_engine.csv;

cd visualize_benchmark;
python3 compare.py operator_engine.csv hardcoded.csv string_engine.csv;
