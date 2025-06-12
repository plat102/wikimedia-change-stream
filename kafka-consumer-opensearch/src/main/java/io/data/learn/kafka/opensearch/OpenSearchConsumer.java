package io.data.learn.kafka.opensearch;

import com.google.gson.JsonParser;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
//        String connString = "http://localhost:9200";
        Dotenv dotenv = Dotenv.load();
        String connString = dotenv.get("OPENSEARCH_URL");

        // build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);

        // extract logic information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme())));
        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Dotenv dotenv = Dotenv.load();
        String bootstrapServer = dotenv.get("KAFKA_BOOTSTRAP_SERVERS");
        String groupId = "consumer-opensearch-demo";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // manual commit
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        return consumer;
    }

    private static String extractID(String json) {
        return JsonParser.parseString(json).getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        try {

            // --- 1. first create an OpenSearch client ---
            RestHighLevelClient openSearchClient = createOpenSearchClient();

            // --- 2. create our Kafka client ---
            KafkaConsumer<String, String> consumer = createKafkaConsumer();

            // create OpenSearch index if it doesn't exist already
            try (openSearchClient; consumer) {
                boolean indexExist = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

                if (!indexExist) {
                    CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                    openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

                    log.info("The Wikimedia Index has been created!");

                } else {
                    log.info("The Wikimedia Index already exits");
                }

                // subscribe topic / assign partition
                consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

                // --- 4. main code logic ---
                while (true) {

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                    int recordCount = records.count();
                    log.info("Received " + recordCount + " record(s");

                    // --- batching data by bulk request ----
                    BulkRequest bulkRequest = new BulkRequest();

                    for (ConsumerRecord<String, String> record : records) {

                        // send the record into OpenSearch via index request

                        // --- send record avoiding msg duplication ---
                        // strategy 1: define and ID using Kafka coordinate
//                        String msgId = record.topic() + "_" + record.partition() + "_" + record.offset();

                        try {
                            // strategy 2: extract Id from JSON value
                            String msgId = extractID(record.value());

                            IndexRequest indexRequest = new IndexRequest("wikimedia")
                                    .source(record.value(), XContentType.JSON)
                                    .id(msgId);

                            // IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
//                            log.info("Inserted 1 document into OpenSearch");
//                            log.info(indexResponse.getId());

                            // add index request to bulk request
                            bulkRequest.add(indexRequest);

                        } catch (Exception e) {
                            log.error("Something went wrong when sending record into OpenSearch", e);
                        }

                    }
                    // send bulk request to OpenSearch
                    if (bulkRequest.numberOfActions() > 0) {
                        BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                        log.info("Inserted " + bulkResponse.getItems().length + " record(s) into OpenSearch");

                        // accept delay
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e){
                            e.printStackTrace();
                        }

                        // --- At least once - commit offset after the batch is consumed ---
                        consumer.commitSync();
                        log.info("Offsets have been commited!");
                    }

                    // --- At least once - commit offset after the record/batch is consumed ---
//                    consumer.commitSync();
//                    log.info("Offsets have been commited!");

                }

            }

            // --- 5. close things ---


        } catch (Exception e) {
            log.error("Something went wrong in OpenSearchConsumer", e);
        }

    }
}
