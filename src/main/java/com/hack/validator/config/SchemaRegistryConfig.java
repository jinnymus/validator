package com.hack.validator.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.schema.client.ConfluentSchemaRegistryClient;
import org.springframework.cloud.stream.schema.client.SchemaRegistryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//@Configuration
public class SchemaRegistryConfig {

    @Value(value = "${spring.cloud.stream.kafka.binder.consumer-properties.schema.registry.url}")
    private String endPoint;

    @Bean
    public SchemaRegistryClient schemaRegistryClient() {
        ConfluentSchemaRegistryClient client = new ConfluentSchemaRegistryClient();
        client.setEndpoint(endPoint);
        return client;
    }
}
