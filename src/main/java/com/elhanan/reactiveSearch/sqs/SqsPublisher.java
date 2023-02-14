package com.elhanan.reactiveSearch.sqs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.messaging.core.QueueMessagingTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SqsPublisher {
    @Autowired
    private QueueMessagingTemplate queueMessagingTemplate;
    @Autowired
    ObjectMapper om;

    @Value("${cloud.aws.end-point.uri}")
    private String endpoint;

    public void send(Object message) throws JsonProcessingException {
        queueMessagingTemplate.convertAndSend(endpoint, message);
    }
}
