package com.elhanan.reactiveSearch.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.util.UUID;

import static com.elhanan.reactiveSearch.kafka.KafkaTopicConfig.APP_TOPIC;

@Component
public class AppKafkaSender {

    @Autowired
    @Qualifier("simpleProducer")
    private KafkaSender<String, String> kafkaSender;

    @Autowired
    ObjectMapper om;

    public boolean send(String msg, String topic) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, null, UUID.randomUUID().toString(),
                msg);
        Mono<SenderRecord<String, String, String>> mono = Mono.just(SenderRecord.create(record, null));
        Flux<SenderResult<String>> res =  kafkaSender.send(mono);
        res.collectList().subscribe();

        return true;
    }

    public boolean send(Object message) throws JsonProcessingException {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(APP_TOPIC, null, UUID.randomUUID().toString(),
                om.writeValueAsString(message));
        Mono<SenderRecord<String, String, String>> mono = Mono.just(SenderRecord.create(record, null));
        Flux<SenderResult<String>> res =  kafkaSender.send(mono);
        res.collectList().subscribe();

        return true;
    }
}
