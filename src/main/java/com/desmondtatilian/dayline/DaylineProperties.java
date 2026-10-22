package com.desmondtatilian.dayline;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dayline")
public class DaylineProperties {

    private String mode = "all";
    private final Kafka kafka = new Kafka();
    private final Dynamo dynamo = new Dynamo();

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public boolean runsApi() {
        return "api".equals(mode) || "all".equals(mode);
    }

    public boolean runsStream() {
        return "stream".equals(mode) || "all".equals(mode);
    }

    public Kafka getKafka() {
        return kafka;
    }

    public Dynamo getDynamo() {
        return dynamo;
    }

    public static class Kafka {
        private String bootstrapServers = "localhost:9092";
        private String eventsTopic = "guest.events";
        private String deadTopic = "guest.events.dlq";

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getEventsTopic() {
            return eventsTopic;
        }

        public void setEventsTopic(String eventsTopic) {
            this.eventsTopic = eventsTopic;
        }

        public String getDeadTopic() {
            return deadTopic;
        }

        public void setDeadTopic(String deadTopic) {
            this.deadTopic = deadTopic;
        }
    }

    public static class Dynamo {
        private String endpoint = "http://localhost:8000";
        private String table = "itinerary";
        private String region = "us-east-1";

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getRegion() {
            return region;
        }

        public void setRegion(String region) {
            this.region = region;
        }
    }
}
