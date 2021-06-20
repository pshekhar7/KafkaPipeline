package com.pshekhar.kafka.kafkapubsub.consumer;

import com.pshekhar.kafka.kafkapubsub.model.Notification;
import com.pshekhar.kafka.kafkapubsub.util.ReceiverRecordException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

@Component
@Slf4j
public class KafkaConsumer implements CommandLineRunner {
    @Autowired
    private KafkaReceiver<Integer, Notification> receiver;

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");

    @Override
    public void run(String... args) throws Exception {
        this.consumeMessages().subscribe();
    }

    private Flux<Notification> consumeMessages() {
        return receiver
                .receive()
                .publishOn(Schedulers.boundedElastic())
                .delayElements(Duration.ofSeconds(2))
                .doOnNext(record -> {
                    log.info("Received message: topic-partition=[{}] offset=[{}] timestamp=[{}] key=[{}] value=[{}]",
                            record.partition(),
                            record.offset(),
                            dateFormat.format(new Date(record.timestamp())),
                            record.key(),
                            record.value());
                    record.receiverOffset().commit().subscribe();
                })
                .onErrorResume(e -> {
                    ReceiverRecordException ex = (ReceiverRecordException) e.getCause();
                    log.error("Retries exhausted for {}", ex.getRecord().value());
                    ex.getRecord().receiverOffset().acknowledge();
                    return Mono.empty();
                })
                .map(ConsumerRecord::value)
                .doOnNext(message -> log.info("Successfully consumed {}={}", Notification.class.getSimpleName(), message))
                .doOnError(throwable -> log.error("Error occurred while consumption: {}", throwable.getLocalizedMessage()));

    }
}
