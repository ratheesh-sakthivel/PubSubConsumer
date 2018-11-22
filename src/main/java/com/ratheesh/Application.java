package com.ratheesh;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.cloud.gcp.pubsub.core.PubSubOperations;
import org.springframework.cloud.gcp.pubsub.integration.AckMode;
import org.springframework.cloud.gcp.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import org.springframework.cloud.gcp.pubsub.support.GcpPubSubHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

@SpringBootApplication
public class Application extends SpringBootServletInitializer {

    private static final Log LOGGER = LogFactory.getLog(Application.class);

    private static BigQuery bigQuery;
    private ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        // Instantiate a client.
        bigQuery = BigQueryOptions.getDefaultInstance().getService();

        SpringApplication.run(Application.class, args);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(Application.class);
    }

    @Bean
    public MessageChannel pubsubInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public PubSubInboundChannelAdapter messageChannelAdapter(
        @Qualifier("pubsubInputChannel") MessageChannel inputChannel,
        PubSubOperations pubSubTemplate) {
        PubSubInboundChannelAdapter adapter =
            new PubSubInboundChannelAdapter(pubSubTemplate, "consumersubscription");
        adapter.setOutputChannel(inputChannel);
        adapter.setAckMode(AckMode.MANUAL);

        return adapter;
    }

    @Bean
    @ServiceActivator(inputChannel = "pubsubInputChannel")
    public MessageHandler messageReceiver() {
        return message -> {
            try {
                String payload = message.getPayload().toString();
                LOGGER.info("Message arrived! Payload: " + payload);
                AckReplyConsumer consumer =
                    (AckReplyConsumer) message.getHeaders().get(GcpPubSubHeaders.ACKNOWLEDGEMENT);
                consumer.ack();

                JsonNode jsonNode = null;

                jsonNode = objectMapper.readTree(payload);

                String datasetName = "my_dataset_1";
                String tableName = "pubsubtobigquery";
                TableId tableId = TableId.of(datasetName, tableName);
                // Values of the row to insert
                Map<String, Object> rowContent = new HashMap<>();
                rowContent.put("step", jsonNode.get("step").asInt());
                rowContent.put("type", jsonNode.get("type").asText());
                rowContent.put("amount", jsonNode.get("amount").asDouble());
                rowContent.put("nameOrig", jsonNode.get("nameOrig").asText());
                rowContent.put("oldbalanceOrg", jsonNode.get("oldbalanceOrg").asDouble());
                rowContent.put("newbalanceOrig", jsonNode.get("newbalanceOrig").asDouble());
                rowContent.put("nameDest", jsonNode.get("nameDest").asText());
                rowContent.put("oldbalanceDest", jsonNode.get("oldbalanceDest").asDouble());
                rowContent.put("newbalanceDest", jsonNode.get("newbalanceDest").asDouble());
                rowContent.put("isFraud", jsonNode.get("isFraud").asInt());
                rowContent.put("isFlaggedFraud", jsonNode.get("isFlaggedFraud").asInt());

                InsertAllResponse response =
                    bigQuery.insertAll(
                        InsertAllRequest.newBuilder(tableId)
                            .addRow(rowContent)
                            // More rows can be added in the same RPC by invoking .addRow() on the builder
                            .build());
                if (response.hasErrors()) {
                    // If any of the insertions failed, this lets you inspect the errors
                    for (Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                        // inspect row error
                        LOGGER.error(entry.getKey() + " " + entry.getValue());
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        };
    }
}
