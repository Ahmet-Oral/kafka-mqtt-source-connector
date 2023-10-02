package com.ahmetoral.kafkacustommqttconnector;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MqttSourceProcessor {

    private final Logger logger = LoggerFactory.getLogger(MqttSourceProcessor.class);
    private MqttSourceConnectorTask task;

    public MqttSourceProcessor(Map<String, String> props, MqttSourceConnectorTask task) {
        this.task = task;
    }

    // define how to process a single MQTT message here
    public void process(String topic, byte[] data) {
        logger.info("Processing message from topic: {}", topic);

        long timestamp = System.currentTimeMillis();

        // your Kafka topic, can be different for different messages
        // note: make sure to configure the schema for different types of messages
        String kafkaTopic = "myKafkaTopic";
        String kafkaErrorTopic = "myKafkaErrorTopic";

        // parse the message
        Object value = data;
        String messageKey = "exampleMessageKey";

        try {
            addRecord(kafkaTopic, timestamp, messageKey, value);
        } catch (Exception e) {
            /* if message could not be serialized to given schema (or any other problems), you can still publish it to
            your error topic in Kafka. Details can be provided through the message headers. */
            task.addBadRecord(kafkaErrorTopic, messageKey, timestamp, topic, data);
        }

    }

    private void addRecord(String topic, long timestamp, String key, Object value) {
        // define the message schema
        Schema valueSchema = null;
        /*
         can alternatively generate the schema from the message itself.
         Example for Protobuf: new ProtobufSchema(ExampleProtoClass.getDescriptor());
        */
        task.addRecord(topic, key, value, valueSchema, timestamp);
    }
}
