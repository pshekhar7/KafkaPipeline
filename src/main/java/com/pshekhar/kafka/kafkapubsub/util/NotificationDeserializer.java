package com.pshekhar.kafka.kafkapubsub.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pshekhar.kafka.kafkapubsub.model.Notification;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class NotificationDeserializer implements Deserializer<Notification> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Notification deserialize(String topic, byte[] bytes) {
        try {
            return mapper.readValue(bytes, Notification.class);
        } catch (IOException e) {
            log.error("Failed to de-serialize Notification message: {}", e.getMessage());
        }
        return null;
    }

    @Override
    public Notification deserialize(String topic, Headers headers, byte[] data) {
        try {
            return mapper.readValue(data, Notification.class);
        } catch (IOException e) {
            log.error("Failed to de-serialize Notification message: {}", e.getMessage());
        }
        return null;
    }

    @Override
    public void close() {

    }
}
