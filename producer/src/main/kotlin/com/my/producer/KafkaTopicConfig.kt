package com.my.producer

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean
    fun userEventsTopic(): NewTopic {
        return TopicBuilder.name("user-events")
            .partitions(3)
            .replicas(1)
            .build()
    }

    @Bean
    fun retailDataTopic(): NewTopic {
        return TopicBuilder.name("retail-data")
            .partitions(5)
            .replicas(1)
            .build()
    }
}
