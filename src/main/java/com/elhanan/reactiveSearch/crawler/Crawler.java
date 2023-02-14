package com.elhanan.reactiveSearch.crawler;

import com.elhanan.reactiveSearch.model.*;
import com.elhanan.reactiveSearch.sqs.SqsPublisher;
import com.elhanan.reactiveSearch.util.ElasticSearch;
import com.elhanan.reactiveSearch.util.ReactiveHTTPClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class Crawler {
    @Autowired
    RedisTemplate redisTemplate;

    @Autowired
    ObjectMapper om;

    @Autowired
    SqsPublisher sqsPublisher;

    @Autowired
    ElasticSearch elasticSearch;

    @Autowired
    ReactiveHTTPClient httpClient;

    protected final Log logger = LogFactory.getLog(getClass());

    public void crawl(String crawlId, CrawlerRequest crawlerRequest) throws IOException {
        initCrawlInRedis(crawlId);
        sqsPublisher.send(CrawlerRecord.of(crawlId, crawlerRequest));
    }

    public void crawlOneRecord(String crawlId, CrawlerRecord rec) throws IOException {
        logger.info("Crawling url: " + rec.getUrl());
        StopReason stopReason = getStopReason(rec);
        setCrawlStatus(crawlId, CrawlStatus.of(rec.getDistance(), rec.getStartTime(), 0, stopReason));
        if (stopReason == null) {
            Runnable handle4xx = () -> {
                try {
                    setCrawlStatus(crawlId, CrawlStatus.of(rec.getDistance(), rec.getStartTime(), 0, StopReason.ERROR_4XX));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            };
            Runnable handle5xx = () -> {
                try {
                    setCrawlStatus(crawlId, CrawlStatus.of(rec.getDistance(), rec.getStartTime(), 0, StopReason.ERROR_5XX));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            };
            httpClient.get(rec.getUrl(), handle4xx, handle5xx)
                    .subscribe(html -> {
                                Document webPageContent = Jsoup.parse(html);
                                indexElasticSearch(rec, webPageContent);
                                List<String> innerUrls = extractWebPageUrls(rec.getBaseUrl(), webPageContent);
                                try {
                                    addUrlsToQueue(rec, innerUrls, rec.getDistance() + 1);
                                } catch (JsonProcessingException e) {
                                    throw new RuntimeException(e);
                                }
                            },
                            error -> logger.error("Received " + error.getMessage() + " while attempting to fetch " + rec.getUrl()));

        }
    }

    private StopReason getStopReason(CrawlerRecord rec) {
        if (rec.getDistance() == rec.getMaxDistance() + 1) return StopReason.MAX_DISTANCE;
        if (getVisitedUrls(rec.getCrawlId()) >= rec.getMaxUrls()) return StopReason.MAX_URLS;
        if (System.currentTimeMillis() >= rec.getMaxTime()) return StopReason.TIMEOUT;
        return null;
    }


    private void addUrlsToQueue(CrawlerRecord rec, List<String> urls, int distance) throws JsonProcessingException {
        logger.info(">> adding urls to queue: distance->" + distance + " amount->" + urls.size());
        for (String url : urls) {
            if (!crawlHasVisited(rec, url)) {
                sqsPublisher.send(CrawlerRecord.of(rec).withUrl(url).withIncDistance());
            }
        }
    }

    private List<String> extractWebPageUrls(String baseUrl, Document webPageContent) {
        List<String> links = webPageContent.select("a[href]")
                .eachAttr("abs:href")
                .stream()
                .filter(url -> url.startsWith(baseUrl))
                .collect(Collectors.toList());
        logger.info(">> extracted->" + links.size() + " links");

        return links;
    }

    private void indexElasticSearch(CrawlerRecord rec, Document webPageContent) {
        logger.info(">> adding elastic search for webPage: " + rec.getUrl());
        String text = String.join(" ", webPageContent.select("a[href]").eachText());
        UrlSearchDoc searchDoc = UrlSearchDoc.of(rec.getCrawlId(), text, rec.getUrl(), rec.getBaseUrl(), rec.getDistance());
        elasticSearch.addData(searchDoc);
    }

    private void initCrawlInRedis(String crawlId) throws JsonProcessingException {
        setCrawlStatus(crawlId, CrawlStatus.of(0, System.currentTimeMillis(), 0, null));
        redisTemplate.opsForValue().set(crawlId + ".urls.count", "1");
    }

    private void setCrawlStatus(String crawlId, CrawlStatus crawlStatus) throws JsonProcessingException {
        redisTemplate.opsForValue().set(crawlId + ".status", om.writeValueAsString(crawlStatus));
    }

    private boolean crawlHasVisited(CrawlerRecord rec, String url) {
        if (redisTemplate.opsForValue().setIfAbsent(rec.getCrawlId() + ".urls." + url, "1")) {
            redisTemplate.opsForValue().increment(rec.getCrawlId() + ".urls.count", 1L);
            return false;
        } else {
            return true;
        }
    }

    private int getVisitedUrls(String crawlId) {
        Object curCount = redisTemplate.opsForValue().get(crawlId + ".urls.count");
        if (curCount == null) return 0;
        return Integer.parseInt(curCount.toString());
    }

    public CrawlStatusOut getCrawlInfo(String crawlId) throws JsonProcessingException {
        CrawlStatus cs = om.readValue(redisTemplate.opsForValue().get(crawlId + ".status").toString(), CrawlStatus.class);
        cs.setNumPages(getVisitedUrls(crawlId));
        return CrawlStatusOut.of(cs);
    }
}
