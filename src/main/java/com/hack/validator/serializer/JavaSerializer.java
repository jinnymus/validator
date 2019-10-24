package com.hack.validator.serializer;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

public class JavaSerializer implements Serializer<Object> {
    @Override
    public byte[] serialize(String topic, Object data) {
        return (byte[]) data;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }
}
