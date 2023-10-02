package com.ahmetoral.kafkacustommqttconnector;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.ahmetoral.kafkacustommqttconnector.MqttSourceConstants.*;


public class MqttSourceConnectorTask extends SourceTask {

    private static final String VERSION = "1.0";
    private static final List<SourceRecord> STATIC_EMPTY_LIST = new ArrayList<>();
    private static final Map<String, Object> STATIC_EMPTY_MAP = new HashMap<>();
    private static int POLL_INTERVAL;

    private final Logger logger = LoggerFactory.getLogger(MqttSourceConnectorTask.class);
    private MqttSourceClient client;
    private MqttSourceProcessor processor;
    private BlockingQueue<SourceRecord> queue;

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting MqttSourceConnectorTask");

        POLL_INTERVAL = Integer.parseInt(props.get(QUEUE_POLL_INTERVAL));
        this.queue = new LinkedBlockingQueue<>();

        this.processor = new MqttSourceProcessor(props, this);
        try {
            this.client = new MqttSourceClient(props, processor);
        } catch (Exception e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // wait for queue to fill for given amount of poll interval.
        SourceRecord sourceRecord = queue.poll(POLL_INTERVAL, TimeUnit.SECONDS);
        if (sourceRecord == null) {
            return STATIC_EMPTY_LIST;
        }
        List<SourceRecord> records = new ArrayList<>();
        records.add(sourceRecord);
        queue.drainTo(records);
        return records;
    }

    @Override
    public void stop() {
        client.close();
        logger.error("Connector stopped");
    }

    public void addRecord(String topic, String key, Object value, Schema valueSchema, Long timestamp) {
        addRecord(topic, key, value, valueSchema, timestamp, null);
    }

    public void addBadRecord(String topic, String key, Long timestamp, String mqttTopic, byte[] data) {
        ConnectHeaders connectHeaders = new ConnectHeaders();
        connectHeaders.addString("topic", mqttTopic);
        connectHeaders.addBytes("data", data);

        addRecord(topic, key, null, null, timestamp, connectHeaders);
    }

    private void addRecord(String topic, String key, Object value, Schema valueSchema, Long timestamp, Iterable<Header> headers) {
        SourceRecord sr = new SourceRecord(
            STATIC_EMPTY_MAP, STATIC_EMPTY_MAP,
            topic, null,
            Schema.STRING_SCHEMA, key,
            valueSchema, value,
            timestamp, headers
        );

        try {
            queue.put(sr);
        } catch (InterruptedException e) {
            queue.offer(sr);
        }
    }

    @Override
    public String version() {
        return VERSION;
    }

}
