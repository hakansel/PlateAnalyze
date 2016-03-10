# PlateAnalyze
It aims to test of usage Apache Storm and Cassandra

This project is maven based java project.

* With initial commit it is constructed static topology configuration. 
* (TODO) But first issue is that this project will be configured with Apache Flux using local.yaml and remote.yaml.
* It accepts streams via tcp socket to spout.
* (TODO) It must accept stream via Apache Fluem and Kafka Spout rather than tcp.
* It writes results to Cassandra if it detects right sequence of plates.

