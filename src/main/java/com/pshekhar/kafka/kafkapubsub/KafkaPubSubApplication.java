package com.pshekhar.kafka.kafkapubsub;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(value = "com.pshekhar.kafka.kafkapubsub.*")
@EnableAutoConfiguration
public class KafkaPubSubApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaPubSubApplication.class, args);
    }

}
