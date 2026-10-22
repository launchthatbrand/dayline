package com.desmondtatilian.dayline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class DaylinePipelineTest {

    @Container
    static final MySQLContainer mysql = new MySQLContainer(DockerImageName.parse("mysql:8.4"));

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    @Container
    static final GenericContainer<?> dynamo = new GenericContainer<>(DockerImageName.parse("amazon/dynamodb-local:2.6.0"))
            .withExposedPorts(8000)
            .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
            .waitingFor(Wait.forListeningPort());

    @Autowired
    private TestRestTemplate rest;

    @org.springframework.test.context.DynamicPropertySource
    static void properties(org.springframework.test.context.DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> mysql.getJdbcUrl() + "?sslMode=DISABLED&allowPublicKeyRetrieval=true");
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
        registry.add("dayline.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("dayline.dynamo.endpoint", () -> "http://" + dynamo.getHost() + ":" + dynamo.getMappedPort(8000));
        registry.add("dayline.mode", () -> "all");
    }

    @Test
    void duplicateTapKeepsOneRedemption() {
        JsonNode window = rest.postForObject(
                "/api/windows",
                Map.of("attraction", "Space Mountain", "startsAt", "2026-10-23T15:00:00Z", "capacity", 2),
                JsonNode.class);
        JsonNode booked = rest.postForObject(
                "/api/reservations",
                Map.of("guestName", "Avery Chen", "windowId", window.get("id").asText()),
                JsonNode.class);
        String guestId = booked.get("guestId").asText();
        awaitItem(guestId, "reservation.booked");

        JsonNode first = rest.postForObject(
                "/api/taps",
                Map.of("guestId", guestId, "windowId", window.get("id").asText(), "tapId", "gate-1"),
                JsonNode.class);
        JsonNode second = rest.postForObject(
                "/api/taps",
                Map.of("guestId", guestId, "windowId", window.get("id").asText(), "tapId", "gate-1"),
                JsonNode.class);

        assertThat(first.get("result").asText()).isEqualTo("REDEEMED");
        assertThat(second.get("result").asText()).isEqualTo("REDEEMED");
        assertThat(second.get("idempotencyKey").asText()).isEqualTo(first.get("idempotencyKey").asText());
        awaitItem(guestId, "tap.redeemed");
        JsonNode stats = rest.getForObject("/api/stats", JsonNode.class);
        JsonNode stored = findWindow(stats, "Space Mountain");
        assertThat(stats.get("taps")).hasSize(1);
        assertThat(stored.get("remaining").asInt()).isEqualTo(1);
    }

    @Test
    void hotelFeedAndCancelReturnInventory() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_XML);
        String xml = """
                <hotelStay guest="Mina Ortiz" attraction="Haunted Mansion" windowStart="2026-10-23T18:00:00Z" capacity="3"/>
                """;
        JsonNode booked = rest.postForObject("/api/feeds/hotel", new HttpEntity<>(xml, headers), JsonNode.class);
        assertThat(booked.get("source").asText()).isEqualTo("xml");
        assertThat(booked.get("status").asText()).isEqualTo("BOOKED");
        awaitItem(booked.get("guestId").asText(), "reservation.booked");

        JsonNode cancelled = rest.postForObject("/api/reservations/" + booked.get("id").asText() + "/cancel", Map.of(), JsonNode.class);
        assertThat(cancelled.get("status").asText()).isEqualTo("CANCELLED");
        JsonNode stats = rest.getForObject("/api/stats", JsonNode.class);
        JsonNode window = findWindow(stats, "Haunted Mansion");
        assertThat(window.get("remaining").asInt()).isEqualTo(window.get("capacity").asInt());
    }

    @Test
    void failedProjectionCanBeReplayed() {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            JsonNode stats = rest.getForObject("/api/stats", JsonNode.class);
            assertThat(stats.get("unpublished").asInt()).isZero();
        });
        rest.put("/api/consumer", Map.of("failNext", true));
        JsonNode window = rest.postForObject(
                "/api/windows",
                Map.of("attraction", "Pirates", "startsAt", "2026-10-23T19:00:00Z", "capacity", 2),
                JsonNode.class);
        JsonNode booked = rest.postForObject(
                "/api/reservations",
                Map.of("guestName", "Jules Park", "windowId", window.get("id").asText()),
                JsonNode.class);
        String guestId = booked.get("guestId").asText();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            JsonNode stats = rest.getForObject("/api/stats", JsonNode.class);
            assertThat(eventIdFor(stats, guestId)).isNotBlank();
        });
        String eventId = eventIdFor(rest.getForObject("/api/stats", JsonNode.class), guestId);
        rest.postForObject("/api/events/" + eventId + "/replay", Map.of(), JsonNode.class);
        awaitItem(guestId, "reservation.booked");
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            JsonNode stats = rest.getForObject("/api/stats", JsonNode.class);
            assertThat(eventIdFor(stats, guestId)).isBlank();
        });
    }

    private JsonNode findWindow(JsonNode stats, String attraction) {
        for (JsonNode candidate : stats.get("windows")) {
            if (attraction.equals(candidate.get("attraction").asText())) {
                return candidate;
            }
        }
        throw new AssertionError("Missing window " + attraction);
    }

    private String eventIdFor(JsonNode stats, String guestId) {
        for (JsonNode dead : stats.get("deadLetters")) {
            if (guestId.equals(dead.get("guestId").asText())) {
                return dead.get("eventId").asText();
            }
        }
        return "";
    }

    private void awaitItem(String guestId, String type) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            JsonNode itinerary = rest.getForObject("/api/itinerary/" + guestId, JsonNode.class);
            assertThat(itinerary.get("items").toString()).contains(type);
        });
    }
}
