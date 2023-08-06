# Kafka Implementation in Python

This repository contains a simple implementation of Kafka-like messaging using Python and Flask. The implementation consists of four main components: `broker.py`, `zookeeper.py`, `producer.py`, and `consumer.py`. These components work together to enable message production, consumption, and distribution in a distributed system.

## Components

### 1. `broker.py`

The `broker.py` file represents a Kafka broker. It uses Flask to create a web server that handles incoming requests from producers and consumers. The broker supports message saving, reading, and leader election. Some key functionalities include:

- Message Saving: Producers can send messages to specific topics and partitions, and the broker saves these messages into appropriate files.
- Message Reading: Consumers can read messages from specific topics, starting from the last read offset or from the beginning if specified.
- Leader Election: The broker participates in leader election using Zookeeper for high availability.

### 2. `zookeeper.py`

The `zookeeper.py` file simulates a Zookeeper-like service for leader election in the Kafka cluster. It also uses Flask to create a web server that interacts with brokers and handles leader election. Some key functionalities include:

- Leader Election: Zookeeper-like functionality is implemented to elect a new leader in case the current leader fails.

### 3. `producer.py`

The `producer.py` file represents a simple Kafka producer. It sends messages to the Kafka brokers and publishes them to specific topics. The producer interacts with the broker using HTTP requests.

### 4. `consumer.py`

The `consumer.py` file represents a simple Kafka consumer. It reads messages from specific topics and partitions and prints them to the console. The consumer can start reading from the beginning or from the last read offset.

## Getting Started

To run this implementation, follow these steps:

1. Start Zookeeper-like service: Run `zookeeper.py` to start the Zookeeper-like service that handles leader election.

2. Start Kafka brokers: Run `broker.py` to start the Kafka brokers. Multiple brokers can be started to form a distributed cluster.

3. Produce Messages: Run `producer.py <topic>` to start a producer and send messages to the specified topic.

4. Consume Messages: Run `consumer.py <topic>` to start a consumer and read messages from the specified topic. Use the `--from-beginning` flag to read from the beginning.

Please ensure you have Flask and the required dependencies installed before running the files.

## Conclusion

This Kafka-like messaging implementation provides a basic but effective way to send and receive messages in a distributed environment. The use of Flask for creating web servers allows easy communication between components. With leader election implemented using Zookeeper-like service, the system maintains high availability and fault tolerance. Feel free to explore and extend the functionalities as per your needs.

**Note:** This implementation is intended for educational and learning purposes. It may not be suitable for production use.
