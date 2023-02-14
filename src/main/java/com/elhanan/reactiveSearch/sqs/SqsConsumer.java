package com.elhanan.reactiveSearch.sqs;

import com.elhanan.reactiveSearch.crawler.Crawler;
import com.elhanan.reactiveSearch.model.CrawlerRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.messaging.listener.SqsMessageDeletionPolicy;
import io.awspring.cloud.messaging.listener.annotation.SqsListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class SqsConsumer {
    @Autowired
    ObjectMapper om;

    @Autowired
    Crawler crawler;

    @SqsListener(value = "searchengine",deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
    public void listen(Object message) throws IOException {
        CrawlerRecord rec = om.readValue(message.toString(), CrawlerRecord.class);
        crawler.crawlOneRecord(rec.getCrawlId(), rec);
    }
}
