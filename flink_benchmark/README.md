# Sample streaming application using flink

## Introduction

This application implements our use case as a flink application.

## Setup

The project is build using the scala build tool (SBT). Download and set up as described in https://www.scala-sbt.org/download.html.
After installing, the sbt shell should be available by running the command _sbt_.
In this dir, run `sbt assembly` to build a fat jar of ~70MB.
Other usefull commands are `sbt clean`, `sbt update` and `sbt reload`.

## Usage

Install flink and startup the cluster using the commands provided in setup_flink.sh located in the root diretory of this repo.
Either submit the previously build jar via CLI or open the web frontend on 127.0.0.1:8080 or 127.0.0.1:8081.
The build will be located in /target/scala-2.11.
In the web frontend choose 'Submit New Job' click 'Add New' and select the jar.

Possible (and aslo necessary) command line options are:
* --ip The host from which to pull the data to process, ususally 127.0.0.1
* --checkoutPort The port on the provided host, which provides the endpoint for checkout data
+ --adPort Dto. for ad data
* --outputPath The file sink for processed data. Attention: If the file is allready present, it will be overwritten without warning

Before submitting the application the data generator must be ready for transmitting data.
