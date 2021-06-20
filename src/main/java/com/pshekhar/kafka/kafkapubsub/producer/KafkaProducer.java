package com.pshekhar.kafka.kafkapubsub.producer;

import com.pshekhar.kafka.kafkapubsub.model.Notification;
import com.pshekhar.kafka.kafkapubsub.model.NotificationResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import javax.annotation.PreDestroy;
import java.text.SimpleDateFormat;
import java.util.Date;

@Component
@Slf4j
public class KafkaProducer {
    @Autowired
    private KafkaSender<Integer, Notification> sender;

    @Value("${notification.kafka.topic}")
    private String topic;

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");

    public Flux<NotificationResponse> publish(final Notification message) {
        return sender.send(Mono
                .just(SenderRecord
                        .create(new ProducerRecord<>(topic, message), Integer.valueOf(message.getId()))))
                .doOnError(throwable -> log.error("Error occurred: {}", throwable.getLocalizedMessage()))
                .map(res -> {
                    RecordMetadata metadata = res.recordMetadata();

                    NotificationResponse notificationResponse = new NotificationResponse(res.correlationMetadata(), metadata.topic(),
                            metadata.partition(),
                            metadata.offset(),
                            dateFormat.format(new Date(metadata.timestamp())));

                    log.info("Message [{}] sent successfully. Topic-Partition=[{}]-[{}] offset=[{}] timestamp=[{}]",
                            notificationResponse.getCorrelationId(),
                            notificationResponse.getTopic(),
                            notificationResponse.getPartition(),
                            notificationResponse.getOffset(),
                            notificationResponse.getTimestamp());

                    return notificationResponse;
                });
    }

    @PreDestroy
    private void cleanup() {
        sender.close();
    }
}
