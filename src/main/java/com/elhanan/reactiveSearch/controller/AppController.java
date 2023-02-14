package com.elhanan.reactiveSearch.controller;

import com.elhanan.reactiveSearch.crawler.Crawler;
import com.elhanan.reactiveSearch.kafka.AppKafkaSender;
import com.elhanan.reactiveSearch.model.CrawlStatusOut;
import com.elhanan.reactiveSearch.model.CrawlerRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.io.IOException;

import static com.elhanan.reactiveSearch.util.UUID.generateRandomID;

@RestController
public class AppController {
    @Autowired
    AppKafkaSender kafkaSender;

    @Autowired
    Crawler crawler;

    @PostMapping(value = "/crawl")
    public String crawl(@RequestBody CrawlerRequest request) {
        String crawlId = generateRandomID();
        if (!request.getUrl().startsWith("http")) {
            request.setUrl("https://" + request.getUrl());
        }
        new Thread(() -> {
            try {
                crawler.crawl(crawlId, request);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        return crawlId;
    }

    @GetMapping(value = "/crawl/{crawlId}")
    public Mono<CrawlStatusOut> getCrawl(@PathVariable String crawlId) throws IOException {
        return Mono.just(crawler.getCrawlInfo(crawlId));
    }
}
