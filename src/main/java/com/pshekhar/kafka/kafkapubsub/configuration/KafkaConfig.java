package com.pshekhar.kafka.kafkapubsub.configuration;

import com.pshekhar.kafka.kafkapubsub.model.Notification;
import com.pshekhar.kafka.kafkapubsub.util.NotificationDeserializer;
import com.pshekhar.kafka.kafkapubsub.util.NotificationSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {
    @Value("${notification.kafka.bootstrap-server}")
    private String bootstrapServer;

    @Value("${notification.kafka.topic}")
    private String topic;

    @Bean
    public KafkaSender<Integer, Notification> kafkaSender() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, NotificationSerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        SenderOptions<Integer, Notification> senderOptions =
                SenderOptions.<Integer, Notification>create(producerProps)
                        .maxInFlight(128);
        return KafkaSender.create(senderOptions);
    }

    @Bean
    public KafkaReceiver<Integer, Notification> kafkaReceiver() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkareactivepubsub");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, NotificationDeserializer.class);

        ReceiverOptions<Integer, Notification> receiverOptions =
                ReceiverOptions.<Integer, Notification>create(consumerProps)
                        .commitInterval(Duration.ZERO)
                        .commitBatchSize(0)
                        .maxCommitAttempts(3)
                        .subscription(Collections.singleton(topic));

        return KafkaReceiver.create(receiverOptions);
    }
}
