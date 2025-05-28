package io.data.learn.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventHandler;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        // connect
        String bootstrapServers = "localhost:9093";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // safe producer config (safe by default in > 23.0 version)
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
//        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");


        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topicName = "wikimedia.recentchanges";
        // build producer event source
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventHandler eventHandler = new WikimediaChangeHandler(producer, topicName);

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        // start the producer
        eventSource.start();

        // producer for 10 mins and block the thread util then
        TimeUnit.MINUTES.sleep(10);
    }

}
