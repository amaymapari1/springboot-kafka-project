package com.amay.springboot;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.amay.springboot.TrustAllCertsClient.getTrustAllCertsClient;

@Service
@Slf4j
public class WikimediaChangesProducer {
    private static final Logger LOGGER= LoggerFactory.getLogger(WikimediaChangesProducer.class);

    private KafkaTemplate<String,String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() throws InterruptedException, NoSuchAlgorithmException, KeyManagementException, IOException {
        String topic = "wikimedia_recentchanges";
        EventHandler eventHandler = new WikimediaChangesHandler(kafkaTemplate, topic);
        String url = "https://stream.wikimedia.org/v2/stream/mediawiki.recentchange";
        OkHttpClient client = getTrustAllCertsClient();
        Request request = new Request.Builder().url(url).build();

        Response response = client.newCall(request).execute();
        if (response.isSuccessful()) {
            EventSource.Builder builder = new EventSource.Builder(eventHandler,URI.create(url));

            // Create a new EventSource instance using the builder
            EventSource eventSource = builder.client(client).build();

            // Start the EventSource
            eventSource.start();
        } else {
            System.err.println("Failed to fetch data: " + response.message());
        }
        LOGGER.info("sending Message");
        TimeUnit.MINUTES.sleep(1);
    }
}
