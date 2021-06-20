package com.pshekhar.kafka.kafkapubsub.controller;

import com.pshekhar.kafka.kafkapubsub.model.Notification;
import com.pshekhar.kafka.kafkapubsub.model.NotificationResponse;
import com.pshekhar.kafka.kafkapubsub.producer.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import javax.annotation.PreDestroy;

@RestController
@RequestMapping("kafka")
@Slf4j
public class PublishMessageController {
    @Autowired
    private KafkaProducer producer;

    @PostMapping(value = "/publish", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Flux<NotificationResponse>> publish(@RequestBody Notification message) {
        log.info("Request: {}", message);
        return ResponseEntity.ok().body(producer.publish(message));
    }
}
