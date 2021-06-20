package com.pshekhar.kafka.kafkapubsub.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pshekhar.kafka.kafkapubsub.model.Notification;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@Slf4j
public class NotificationSerializer implements Serializer<Notification> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Notification notification) {
        try {
            return mapper.writeValueAsBytes(notification);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize Notification message: {}", e.getMessage());
        }
        return new byte[0];
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Notification data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize Notification message: {}", e.getMessage());
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
