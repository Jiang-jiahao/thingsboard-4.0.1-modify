package com.jnks.iot.server.queue;

public interface JnksIotQueueAdmin {

    default void createTopicIfNotExists(String topic) {
        createTopicIfNotExists(topic, null);
    }

    void createTopicIfNotExists(String topic, String properties);

    void destroy();

    void deleteTopic(String topic);
}
