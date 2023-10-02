package com.ahmetoral.kafkacustommqttconnector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ahmetoral.kafkacustommqttconnector.MqttSourceConstants.*;

public class MqttSourceConnector extends SourceConnector {

    private static final Logger logger = LoggerFactory.getLogger(MqttSourceConnector.class);
    private static final String VERSION = "1.0";
    private static final String ADDRESSES_SPLITTER = "\\s*,\\s*";
    private static final String ADDRESS_PATTERN = "^(?:(\\w+)://)?([^:]+):(\\d+)$";

    private Map<String, String> connectorProperties;

    @Override
    public void start(Map<String, String> props) {
        this.connectorProperties = props;
        logger.info("STARTING mqtt source connector");
    }

    // to configure multiple tasks
    @Override
    public List<Map<String, String>> taskConfigs(int taskCount) {
        List<Map<String, String>> taskConfigs = new ArrayList<>(taskCount);
        String clientId = connectorProperties.get(MQTT_CLIENT_ID);
        String[] serverAddressesArray = connectorProperties.get(MQTT_SERVER_ADDRESSES).split(ADDRESSES_SPLITTER);
        Pattern pattern = Pattern.compile(ADDRESS_PATTERN);
        int numberOfAddresses = serverAddressesArray.length;
        for (int i = 0; i < taskCount; i++) {
            Map<String, String> modifiedConfig = new HashMap<>(connectorProperties);
            String perTaskClientId = clientId + "-task" + i;
            // to equally balance the load through the list of brokers
            int addressIndex = i % numberOfAddresses;
            String serverAddress = serverAddressesArray[addressIndex];
            Matcher matcher = pattern.matcher(serverAddress);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid Server Address: " + serverAddress +" - Client: " + perTaskClientId);
            }
            String serverHost = matcher.group(2);
            String serverPort = matcher.group(3);
            modifiedConfig.put(MQTT_CLIENT_ID, perTaskClientId);
            modifiedConfig.put(INTERNAL_SERVER_HOST, serverHost);
            modifiedConfig.put(INTERNAL_SERVER_PORT, serverPort);

            taskConfigs.add(modifiedConfig);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("Stopping mqtt source connector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MqttSourceConnectorTask.class;
    }

    @Override
    public ConfigDef config() {
        return MqttSourceConnectorConfig.config;
    }

    @Override
    public String version() {
        return VERSION;
    }

}
