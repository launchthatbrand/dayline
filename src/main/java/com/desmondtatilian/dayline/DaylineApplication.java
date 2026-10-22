package com.desmondtatilian.dayline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication(exclude = KafkaAutoConfiguration.class)
@EnableConfigurationProperties(DaylineProperties.class)
public class DaylineApplication {

    public static void main(String[] args) {
        SpringApplication.run(DaylineApplication.class, args);
    }
}
