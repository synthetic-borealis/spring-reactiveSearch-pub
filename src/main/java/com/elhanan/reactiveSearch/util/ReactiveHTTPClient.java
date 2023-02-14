package com.elhanan.reactiveSearch.util;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.reactive.JettyClientHttpConnector;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
public class ReactiveHTTPClient {
    private final Logger logger = LoggerFactory.getLogger(ReactiveHTTPClient.class);
    private final WebClient webClient;

    public ReactiveHTTPClient() throws Exception {
        SslContextFactory.Client sslContextFactory = new SslContextFactory.Client();
        HttpClient httpClient = new HttpClient(sslContextFactory);
        httpClient.start();
        final int size = 32 * 1024 * 1024;
        final ExchangeStrategies strategies = ExchangeStrategies.builder()
                .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(size))
                .build();
        webClient = WebClient.builder()
                .exchangeStrategies(strategies)
                .clientConnector(new JettyClientHttpConnector(httpClient))
                .build();
    }

    public Mono<String> get(String url, Runnable onError4xx, Runnable onError5xx) {
        logger.info("Fetching resource from: " + url);
        if (onError4xx != null && onError5xx != null) {
            return webClient.get().uri(url).retrieve()
                    .onStatus(HttpStatus::is4xxClientError, clientResponse -> {
                        onError4xx.run();
                        return Mono.error(new RuntimeException("error " + clientResponse.statusCode().value()));
                    })
                    .onStatus(HttpStatus::is5xxServerError, clientResponse -> {
                        onError5xx.run();
                        return Mono.error(new RuntimeException("5xx error"));
                    })
                    .bodyToMono(String.class);
        }
        if (onError4xx != null) {
            return webClient.get().uri(url).retrieve()
                    .onStatus(HttpStatus::is4xxClientError, clientResponse -> {
                        onError4xx.run();
                        return Mono.error(new RuntimeException("4xx error"));
                    })
                    .bodyToMono(String.class);
        }
        if (onError5xx != null) {
            return webClient.get().uri(url).retrieve()
                    .onStatus(HttpStatus::is5xxServerError, clientResponse -> {
                        onError5xx.run();
                        return Mono.error(new RuntimeException("5xx error"));
                    })
                    .bodyToMono(String.class);
        }
        return webClient.get().uri(url).retrieve().bodyToMono(String.class);
    }
}
