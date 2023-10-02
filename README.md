## Overview

The Kafka MQTT Source Connector is a Java-based Kafka Connect connector that can be deployed to any Kafka environment. It is designed to subscribe to MQTT brokers via the HiveMQ interface and process incoming messages, allowing you to seamlessly integrate MQTT data into your Kafka ecosystem.

## Features

- Connects to MQTT brokers via HiveMQ.
- Configurable message processing into Kafka given topics.
- Auto configured to handle MQTT connecions and message processing tasks for horizontally scaling
- Integratable to Kafka Connect and Confluent.

## Connector Configuration

The Kafka MQTT Source Connector can be configured using the following properties:

- **MQTT Server Addresses** (`mqtt.server.addresses`):
  - Type: STRING
  - Importance: HIGH
  - Description: MQTT server addresses. The load will be balanced between the number of tasks and brokers. Should be in the format of `ssl://example-host:port || example-host:port`.

- **MQTT Client ID** (`mqtt.client.id`):
  - Type: STRING
  - Importance: HIGH
  - Description: MQTT client id to use. The client id will be unique across all tasks in the format of: `clientId + taskNumber`.

- **MQTT Topic** (`mqtt.topic`):
  - Type: STRING
  - Importance: HIGH
  - Description: MQTT topic to subscribe to.

- **MQTT Username** (`mqtt.username`):
  - Type: STRING
  - Importance: HIGH
  - Description: MQTT username.

- **MQTT Password** (`mqtt.password`):
  - Type: PASSWORD
  - Importance: HIGH
  - Description: MQTT password.

- **MQTT Quality of Service (QoS)** (`mqtt.qos`):
  - Type: INT
  - Importance: HIGH
  - Description: MQTT Quality of Service (QoS) level.

- **Enable MQTT Clean Session** (`mqtt.clean.session`):
  - Type: BOOLEAN
  - Importance: HIGH
  - Description: Enable MQTT clean session.

- **MQTT Connection Timeout** (`mqtt.connection.timeout`):
  - Type: INT
  - Importance: HIGH
  - Description: MQTT connection timeout in seconds.

- **MQTT Keep-Alive Interval** (`mqtt.keep.alive.interval`):
  - Type: INT
  - Importance: HIGH
  - Description: MQTT keep-alive interval in seconds.

- **Queue Poll Interval** (`queue.poll.interval`):
  - Type: INT
  - Importance: HIGH
  - Description: Queue poll interval in seconds.
