package io.data.learn.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class WikimediaChangeHandler implements EventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String topic;
    private final Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
        this.kafkaProducer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        // nothing
    }

    @Override
    public void onClosed() throws Exception {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        logger.info(messageEvent.getData());

        // asynchronous send the msg
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {
        // nothing
    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("Error in Stream reading", throwable);
    }
}
