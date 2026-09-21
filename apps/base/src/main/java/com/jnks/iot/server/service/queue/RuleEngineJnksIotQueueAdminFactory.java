package com.jnks.iot.server.service.queue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.jnks.iot.server.queue.JnksIotQueueAdmin;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaAdmin;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaSettings;
import com.jnks.iot.server.queue.kafka.JnksIotKafkaTopicConfigs;

@Configuration
public class RuleEngineJnksIotQueueAdminFactory {

    @Autowired(required = false)
    private JnksIotKafkaTopicConfigs kafkaTopicConfigs;
    @Autowired(required = false)
    private JnksIotKafkaSettings kafkaSettings;

    @ConditionalOnExpression("'${queue.type:null}'=='kafka'")
    @Bean
    public JnksIotQueueAdmin createKafkaAdmin() {
        return new JnksIotKafkaAdmin(kafkaSettings, kafkaTopicConfigs.getRuleEngineConfigs());
    }

    @ConditionalOnExpression("'${queue.type:null}'=='in-memory'")
    @Bean
    public JnksIotQueueAdmin createInMemoryAdmin() {
        return new JnksIotQueueAdmin() {

            @Override
            public void createTopicIfNotExists(String topic, String properties) {
            }

            @Override
            public void deleteTopic(String topic) {
            }

            @Override
            public void destroy() {
            }
        };
    }
}
