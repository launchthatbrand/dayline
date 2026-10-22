package com.desmondtatilian.dayline.stream;

import com.desmondtatilian.dayline.DaylineProperties;
import com.desmondtatilian.dayline.ledger.Views;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

@Component
public class ItineraryStore implements AutoCloseable {

    private final String table;
    private final ObjectMapper json;
    private final DynamoDbClient sync;
    private final DynamoDbAsyncClient async;

    public ItineraryStore(DaylineProperties properties, ObjectMapper json) {
        this.table = properties.getDynamo().getTable();
        this.json = json;
        StaticCredentialsProvider credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local"));
        URI endpoint = URI.create(properties.getDynamo().getEndpoint());
        Region region = Region.of(properties.getDynamo().getRegion());
        this.sync = DynamoDbClient.builder()
                .endpointOverride(endpoint)
                .region(region)
                .credentialsProvider(credentials)
                .build();
        this.async = DynamoDbAsyncClient.builder()
                .endpointOverride(endpoint)
                .region(region)
                .credentialsProvider(credentials)
                .build();
        ensureTableWithRetry();
    }

    private void ensureTableWithRetry() {
        RuntimeException last = null;
        for (int attempt = 1; attempt <= 30; attempt++) {
            try {
                ensureTable();
                return;
            } catch (RuntimeException exception) {
                last = exception;
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("DynamoDB table declaration interrupted", interrupted);
                }
            }
        }
        throw last;
    }

    public CompletableFuture<Void> put(String payload) {
        try {
            JsonNode event = json.readTree(payload);
            String guestId = text(event, "guestId");
            String eventId = text(event, "eventId");
            String at = text(event, "at");
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("guestId", AttributeValue.fromS(guestId));
            item.put("sk", AttributeValue.fromS(at + "#" + eventId));
            putString(item, "eventId", eventId);
            putString(item, "type", text(event, "type"));
            putString(item, "status", text(event, "status"));
            putString(item, "guestName", text(event, "guestName"));
            putString(item, "attraction", text(event, "attraction"));
            putString(item, "startsAt", text(event, "startsAt"));
            putString(item, "windowId", text(event, "windowId"));
            putString(item, "reservationId", text(event, "reservationId"));
            return async.putItem(PutItemRequest.builder().tableName(table).item(item).build())
                    .thenApply(response -> null);
        } catch (Exception exception) {
            return CompletableFuture.failedFuture(exception);
        }
    }

    public List<Views.ItineraryItem> list(String guestId) {
        var response = sync.query(QueryRequest.builder()
                .tableName(table)
                .keyConditionExpression("guestId = :guestId")
                .expressionAttributeValues(Map.of(":guestId", AttributeValue.fromS(guestId)))
                .build());
        List<Views.ItineraryItem> items = new ArrayList<>();
        for (Map<String, AttributeValue> item : response.items()) {
            items.add(new Views.ItineraryItem(
                    value(item, "guestId"),
                    value(item, "sk"),
                    value(item, "eventId"),
                    value(item, "type"),
                    value(item, "status"),
                    value(item, "guestName"),
                    value(item, "attraction"),
                    value(item, "startsAt"),
                    value(item, "windowId"),
                    value(item, "reservationId")));
        }
        return items;
    }

    private void ensureTable() {
        try {
            sync.describeTable(request -> request.tableName(table));
        } catch (ResourceNotFoundException missing) {
            sync.createTable(CreateTableRequest.builder()
                    .tableName(table)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("guestId").attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.S).build())
                    .keySchema(
                            KeySchemaElement.builder().attributeName("guestId").keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build())
                    .build());
            sync.waiter().waitUntilTableExists(request -> request.tableName(table));
        } catch (ResourceInUseException busy) {
            sync.waiter().waitUntilTableExists(request -> request.tableName(table));
        }
    }

    private void putString(Map<String, AttributeValue> item, String name, String value) {
        item.put(name, AttributeValue.fromS(value == null ? "" : value));
    }

    private String text(JsonNode event, String field) {
        JsonNode node = event.get(field);
        return node == null || node.isNull() ? "" : node.asText();
    }

    private String value(Map<String, AttributeValue> item, String name) {
        AttributeValue attribute = item.get(name);
        return attribute == null || attribute.s() == null ? "" : attribute.s();
    }

    @Override
    public void close() {
        sync.close();
        async.close();
    }
}
