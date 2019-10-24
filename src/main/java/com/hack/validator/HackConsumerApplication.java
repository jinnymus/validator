package com.hack.validator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.schema.client.EnableSchemaRegistryClient;

@SpringBootApplication
@EnableBinding(Processor.class)
@EnableSchemaRegistryClient
public class HackConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(HackConsumerApplication.class, args);
    }
}