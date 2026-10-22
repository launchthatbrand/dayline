package com.desmondtatilian.dayline.stream;

import com.desmondtatilian.dayline.DaylineProperties;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.stereotype.Component;

@Component
public class KafkaTopics {

    public KafkaTopics(DaylineProperties properties) {
        Properties adminProperties = new Properties();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        adminProperties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        adminProperties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
        IllegalStateException last = null;
        for (int attempt = 1; attempt <= 30; attempt++) {
            try (AdminClient admin = AdminClient.create(adminProperties)) {
                admin.createTopics(List.of(
                                new NewTopic(properties.getKafka().getEventsTopic(), 3, (short) 1),
                                new NewTopic(properties.getKafka().getDeadTopic(), 1, (short) 1)))
                        .all()
                        .get();
                return;
            } catch (ExecutionException exception) {
                if (causedByTopicExists(exception)) {
                    return;
                }
                last = new IllegalStateException("Could not declare Kafka topics", exception);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Kafka topic declaration interrupted", exception);
            } catch (Exception exception) {
                last = new IllegalStateException("Could not declare Kafka topics", exception);
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Kafka topic declaration interrupted", interrupted);
            }
        }
        throw last;
    }

    private boolean causedByTopicExists(Throwable exception) {
        Throwable current = exception;
        while (current != null) {
            if (current instanceof TopicExistsException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    public static Map<String, Object> consumerProperties(String bootstrapServers) {
        return Map.of(
                "bootstrap.servers", bootstrapServers,
                "group.id", "dayline-stream",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "auto.offset.reset", "earliest",
                "enable.auto.commit", "false",
                "group.initial.rebalance.delay.ms", "0");
    }
}
