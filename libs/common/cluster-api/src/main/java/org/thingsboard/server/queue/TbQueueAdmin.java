package org.thingsboard.server.queue;

public interface TbQueueAdmin {

    default void createTopicIfNotExists(String topic) {
        createTopicIfNotExists(topic, null);
    }

    void createTopicIfNotExists(String topic, String properties);

    void destroy();

    void deleteTopic(String topic);
}
