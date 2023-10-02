package com.ahmetoral.kafkacustommqttconnector;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientConfig;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.ahmetoral.kafkacustommqttconnector.MqttSourceConstants.*;

public class MqttSourceClient {

    private final Logger logger = LoggerFactory.getLogger(MqttSourceClient.class);
    private Mqtt5BlockingClient mqttClient;
    private MqttSourceProcessor processor;
    private boolean isEverConnected = false;


    public MqttSourceClient(Map<String, String> props, MqttSourceProcessor processor) {
        this.processor = processor;

        /* note: if you are running the connector with multiple tasks, make sure to use a shared topic to prevent processing
        same messages.*/
        String mqttTopic = props.get(MQTT_TOPIC);
        String mqttClientId = props.get(MQTT_CLIENT_ID);
        String username = props.get(MQTT_USERNAME);
        byte[] password = props.get(MQTT_PASSWORD).getBytes();
        int mqttQos = Integer.parseInt(props.get(MQTT_QOS));
        int connectionTimeout = Integer.parseInt(props.get(MQTT_CONNECTION_TIMEOUT));
        int keepAliveInterval = Integer.parseInt(props.get(MQTT_KEEP_ALIVE_INTERVAL));
        boolean cleanSession = Boolean.parseBoolean(props.get(MQTT_CLEAN_SESSION));
        String serverHost = props.get(INTERNAL_SERVER_HOST);
        int serverPort = Integer.parseInt(props.get(INTERNAL_SERVER_PORT));

        buildMqttClient(mqttClientId, serverHost, serverPort, username, password, connectionTimeout);
        mqttClient.toAsync().publishes(MqttGlobalPublishFilter.ALL, this::processMqttMessage);
        try {
            openMqttConnection(keepAliveInterval, cleanSession);
            subscribeToMqttTopic(mqttTopic, mqttQos);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        logger.info("Successfully subscribed to Topic: {}, Server: {}:{}, Client Id: {}",
                mqttTopic, serverHost, serverPort, mqttClientId);
    }

    private void buildMqttClient(String mqttClientId, String serverHost, int serverPort, String username, byte[] password, int connectionTimeout) {
        logger.info("Attempting to build client. ClientId: {}, Host: {}:{}", mqttClientId, serverHost, serverPort);
        this.mqttClient = MqttClient.builder()
                .useMqttVersion5()
                .identifier(mqttClientId)
                .simpleAuth()
                    .username(username)
                    .password(password)
                .applySimpleAuth()
                .transportConfig()
                    .serverHost(serverHost)
                    .serverPort(serverPort)
                    .sslWithDefaultConfig()
                    .mqttConnectTimeout(connectionTimeout, TimeUnit.SECONDS)
                .applyTransportConfig()
                .automaticReconnectWithDefaultConfig()
                .addConnectedListener(context -> {
                    this.isEverConnected = true;
                    MqttClientConfig config = context.getClientConfig();
                    logger.info("Client Connected. Server: {}:{}, Client Id: {}", config.getServerHost(), config.getServerPort(), config.getClientIdentifier());
                })
                .addDisconnectedListener(context -> {
                    if (!this.isEverConnected) {
                        logger.error("Disconnect listener: MQTT connect Server: {}:{}, Client Id: {}. Cause: {}",
                                serverHost, serverPort, mqttClientId, context.getCause().getMessage());
                        throw new RuntimeException(context.getCause());
                    }
                    MqttClientConfig config = context.getClientConfig();
                    logger.info("Client Disconnected. Server: {}:{}, Client Id: {}. Cause: {}", config.getServerHost(), config.getServerPort(), config.getClientIdentifier(), context.getCause().getMessage());
                })
                .buildBlocking();
    }

    private void openMqttConnection(int keepAliveInterval, boolean cleanSession) {
        MqttClientConfig config = this.mqttClient.getConfig();
        logger.info("Attempting to connect to Server: {}:{}, Client Id: {}", config.getServerHost(), config.getServerPort(), config.getClientIdentifier());
        // Can throw 4 exceptions
        try {
            this.mqttClient.connectWith()
                    .keepAlive(keepAliveInterval)
                    .cleanStart(cleanSession)
                    .send();
        } catch (Exception e) {
            logger.error("Failed to connect to Server: {}:{}, Client Id: {}", config.getServerHost(), config.getServerPort(), config.getClientIdentifier());
            throw e;
        }
    }

    private void subscribeToMqttTopic(String mqttTopic, int mqttQos) {
        MqttClientConfig config = this.mqttClient.getConfig();
        logger.info("Attempting to subscribe to Topic: {}, Server: {}:{}, Client Id: {}",
                mqttTopic, config.getServerHost(), config.getServerPort(), config.getClientIdentifier());
        // Can throw 1 exception
        try {
            this.mqttClient.subscribeWith()
                    .topicFilter(mqttTopic)
                    .qos(MqttQos.fromCode(mqttQos))
                    .send();
        } catch (Exception e) {
            logger.error("Failed to subscribe to Topic: {}, Server: {}:{}, Client Id: {}",
                    mqttTopic, config.getServerHost(), config.getServerPort(), config.getClientIdentifier());
            throw e;
        }
    }

    // callback method for mqtt client
    private void processMqttMessage(Mqtt5Publish publish) {
        String topic = publish.getTopic().toString();
        byte[] bytes = publish.getPayloadAsBytes();
//        logger.debug("Mqtt message arrived to topic: '{}'.", topic);
        processor.process(topic, bytes);
    }

    public void close() {
        this.mqttClient.disconnect();
    }

}
