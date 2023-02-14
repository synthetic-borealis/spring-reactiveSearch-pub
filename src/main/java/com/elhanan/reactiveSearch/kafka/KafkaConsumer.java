package com.elhanan.reactiveSearch.kafka;

import com.elhanan.reactiveSearch.crawler.Crawler;
import com.elhanan.reactiveSearch.model.CrawlerRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Service
public class KafkaConsumer {

    private final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private final KafkaReceiver<String, String> consumer;

    @Autowired
    ObjectMapper om;

    @Autowired
    Crawler crawler;

    public KafkaConsumer(KafkaReceiver<String, String> consumer) {
        this.consumer = consumer;
    }

    @PostConstruct
    public void consume() {
        consumer.receiveAutoAck()
                .subscribe(this::handleMessage);
    }

    private void handleMessage(Flux<ConsumerRecord<String, String>> consumerRecordFlux) {
        consumerRecordFlux.toStream().forEach(fluxRecord -> {
            try {
                CrawlerRecord rec = om.readValue(fluxRecord.value(), CrawlerRecord.class);
                crawler.crawlOneRecord(rec.getCrawlId(), rec);
            } catch (IOException e) {
                logger.error("Cannot deserialize to CrawlerRecord: {}", fluxRecord.value());
            }
        });
    }
}

