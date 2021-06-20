package com.pshekhar.kafka.kafkapubsub.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Notification {
    private int id;
    private String receiverId;
    private String email;
    private String template;
}
