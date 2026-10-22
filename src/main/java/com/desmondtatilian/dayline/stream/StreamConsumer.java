package com.desmondtatilian.dayline.stream;

import com.desmondtatilian.dayline.DaylineProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

@Component
@ConditionalOnExpression("'${dayline.mode}' == 'stream' or '${dayline.mode}' == 'all'")
public class StreamConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamConsumer.class);

    private final JdbcTemplate jdbc;
    private final ItineraryStore itinerary;
    private final ObjectMapper json;
    private final String deadTopic;
    private final KafkaProducer<String, String> deadLetters;
    private final AtomicReference<String> poisoned = new AtomicReference<>();
    private final Set<String> released = ConcurrentHashMap.newKeySet();
    private final Disposable subscription;

    public StreamConsumer(
            JdbcTemplate jdbc,
            ItineraryStore itinerary,
            ObjectMapper json,
            DaylineProperties properties,
            KafkaTopics topics) {
        Objects.requireNonNull(topics);
        this.jdbc = jdbc;
        this.itinerary = itinerary;
        this.json = json;
        this.deadTopic = properties.getKafka().getDeadTopic();
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        this.deadLetters = new KafkaProducer<>(producerProperties);
        ReceiverOptions<String, String> options = ReceiverOptions
                .<String, String>create(KafkaTopics.consumerProperties(properties.getKafka().getBootstrapServers()))
                .subscription(List.of(properties.getKafka().getEventsTopic()));
        this.subscription = KafkaReceiver.create(options)
                .receive()
                .flatMap(this::handle, 4)
                .subscribe(
                        ignored -> {
                        },
                        error -> log.error("Stream consumer stopped", error));
    }

    private Mono<Void> handle(ReceiverRecord<String, String> record) {
        return project(record)
                .retryWhen(Retry.backoff(2, Duration.ofMillis(200)))
                .onErrorResume(error -> deadLetter(record, error))
                .then(Mono.fromRunnable(() -> record.receiverOffset().acknowledge()));
    }

    private Mono<Void> project(ReceiverRecord<String, String> record) {
        String eventId = eventId(record.value());
        return Mono.fromCallable(() -> shouldFail(eventId))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(fail -> {
                    if (fail) {
                        return Mono.error(new ProjectionException("fail switch enabled"));
                    }
                    return Mono.fromFuture(() -> itinerary.put(record.value()));
                })
                .then(Mono.fromRunnable(() -> jdbc.update("DELETE FROM dead_letters WHERE event_id = ?", eventId))
                        .subscribeOn(Schedulers.boundedElastic()))
                .then();
    }

    private boolean shouldFail(String eventId) {
        if (released.contains(eventId)) {
            return false;
        }
        if (eventId.equals(poisoned.get())) {
            return true;
        }
        int claimed = jdbc.update(
                "UPDATE consumer_control SET fail_next = 0 WHERE id = 1 AND fail_next = 1");
        if (claimed == 1) {
            poisoned.set(eventId);
            return true;
        }
        return false;
    }

    private Mono<Void> deadLetter(ReceiverRecord<String, String> record, Throwable error) {
        String eventId = eventId(record.value());
        poisoned.compareAndSet(eventId, null);
        released.add(eventId);
        return Mono.fromCallable(() -> {
                    JsonNode event = json.readTree(record.value());
                    String message = error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
                    if (message.length() > 500) {
                        message = message.substring(0, 500);
                    }
                    jdbc.update(
                            """
                            INSERT INTO dead_letters (event_id, guest_id, event_type, payload, error_message, created_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ON DUPLICATE KEY UPDATE error_message = VALUES(error_message), created_at = VALUES(created_at)
                            """,
                            eventId,
                            text(event, "guestId"),
                            text(event, "type"),
                            record.value(),
                            message,
                            Instant.now().toString());
                    deadLetters.send(new ProducerRecord<>(deadTopic, text(event, "guestId"), record.value())).get();
                    log.info("Dead-lettered {} after {}", eventId, message);
                    return true;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    private String eventId(String payload) {
        try {
            return text(json.readTree(payload), "eventId");
        } catch (Exception exception) {
            return "";
        }
    }

    private String text(JsonNode event, String field) {
        JsonNode node = event.get(field);
        return node == null || node.isNull() ? "" : node.asText();
    }

    @Override
    public void close() {
        subscription.dispose();
        deadLetters.close(Duration.ofSeconds(5));
    }

    private static class ProjectionException extends RuntimeException {
        ProjectionException(String message) {
            super(message);
        }
    }
}
