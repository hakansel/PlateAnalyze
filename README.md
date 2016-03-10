# PlateAnalyze
It aims to test of usage [Apache Storm](https://github.com/apache/storm) and [Cassandra](http://cassandra.apache.org/)

This project is maven based java project.

* With initial commit it is constructed static topology configuration. 
* (TODO) But first issue is that this project will be configured with [Apache Flux](https://github.com/apache/storm/tree/master/external/flux) using local.yaml and remote.yaml.
* It accepts streams via tcp socket to spout.
* (TODO) It must accept stream via [Apache Flume](https://flume.apache.org/) and [Kafka Spout](https://github.com/apache/storm/tree/master/external/storm-kafka) rather than tcp.
* It writes results to Cassandra if it detects right sequence of plates.

