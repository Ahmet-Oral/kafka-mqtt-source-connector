package com.ahmetoral.kafkacustommqttconnector;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

import static com.ahmetoral.kafkacustommqttconnector.MqttSourceConstants.*;

public class MqttSourceConnectorConfig extends AbstractConfig {

    public static final ConfigDef config;

    static {
        config = new ConfigDef().define(
                MQTT_SERVER_ADDRESSES,
                Type.STRING,
                Importance.HIGH,
                "MQTT server addresses. The load will be balanced between number of tasks and brokers. " +
                        "Should be in format of ssl://example-host:port || example-host:port"
        ).define(
                MQTT_CLIENT_ID,
                Type.STRING,
                Importance.HIGH,
                "MQTT client id to use. The client id will be unique across all tasks in format of: clientId + taskNumber"
        ).define(
                MQTT_TOPIC,
                Type.STRING,
                Importance.HIGH,
                "MQTT topic to subscribe to"
        ).define(
                MQTT_USERNAME,
                Type.STRING,
                Importance.HIGH,
                "MQTT username"
        ).define(
                MQTT_PASSWORD,
                Type.PASSWORD,
                Importance.HIGH,
                "MQTT password"
        ).define(
                MQTT_QOS,
                Type.INT,
                Importance.HIGH,
                "MQTT Quality of Service (QoS) level"
        ).define(
                MQTT_CLEAN_SESSION,
                Type.BOOLEAN,
                Importance.HIGH,
                "Enable MQTT clean session"
        ).define(
                MQTT_CONNECTION_TIMEOUT,
                Type.INT,
                Importance.HIGH,
                "MQTT connection timeout in seconds"
        ).define(
                MQTT_KEEP_ALIVE_INTERVAL,
                Type.INT,
                Importance.HIGH,
                "MQTT keep-alive interval in seconds"
        ).define(
                QUEUE_POLL_INTERVAL,
                Type.INT,
                Importance.HIGH,
                "Queue poll interval in seconds"
        );
    }


    public MqttSourceConnectorConfig(Map<String, String> props) {
        super(config, props);
    }

}
