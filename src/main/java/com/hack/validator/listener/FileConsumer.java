package com.hack.validator.listener;

import com.hack.validator.liba2.Duplicator;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class FileConsumer {

    private Duplicator duplicator;

    @StreamListener(value = Processor.INPUT)
    @SendTo(value = Processor.OUTPUT)
    public String consume(String file) {
        //System.out.println("Received: " + file);
        if (duplicator.isDuplicated(file)) {
            return null;
        }
        return file;
    }

    @PostConstruct
    private void postConstruct() {
        duplicator = new Duplicator();
    }
}
