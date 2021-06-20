package com.pshekhar.kafka.kafkapubsub.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class NotificationResponse {
    private int correlationId;
    private String topic;
    private int partition;
    private long offset;
    private String timestamp;
}
