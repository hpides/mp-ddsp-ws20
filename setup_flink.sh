#!/bin/sh
# Install Flink
# https://ci.apache.org/projects/flink/flink-docs-release-1.11/try-flink/local_installation.html
apt install default-jre &&
wget -N https://apache.lauf-forum.at/flink/flink-1.11.2/flink-1.11.2-bin-scala_2.11.tgz &&
tar --skip-old-files -xzf flink-1.11.2-bin-scala_2.11.tgz;

# Im flink Ordner
# ./bin/start-cluster.sh
# ./bin/flink run examples/streaming/WordCount.jar
# tail log/flink-*-taskexecutor-*.out
# ./bin/stop-cluster.sh
