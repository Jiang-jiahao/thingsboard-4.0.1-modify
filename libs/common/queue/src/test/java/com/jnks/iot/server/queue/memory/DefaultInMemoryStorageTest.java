package com.jnks.iot.server.queue.memory;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import com.jnks.iot.server.queue.JnksIotQueueMsg;
import com.jnks.iot.server.queue.common.DefaultJnksIotQueueMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@Slf4j
public class DefaultInMemoryStorageTest {
    static final int MAX_POLL_SIZE = 1000;
    final Gson gson = new Gson();
    final String topic = "jnks_iot_core_notification.jnks-iot-node-0";

    InMemoryStorage storage = new DefaultInMemoryStorage();

    @Test
    public void givenStorage_whenGetLagTotal_thenReturnInteger() throws InterruptedException {
        assertThat(storage.getLagTotal()).isEqualTo(0);
        storage.put("main", mock(JnksIotQueueMsg.class));
        assertThat(storage.getLagTotal()).isEqualTo(1);
        storage.put("main", mock(JnksIotQueueMsg.class));
        assertThat(storage.getLagTotal()).isEqualTo(2);
        storage.put("hp", mock(JnksIotQueueMsg.class));
        assertThat(storage.getLagTotal()).isEqualTo(3);
        storage.get("main");
        assertThat(storage.getLagTotal()).isEqualTo(1);
    }

    @Test
    public void givenQueueWithMoreThenBatchSize_whenPoll_thenReturnFullListAndSecondList() throws InterruptedException {
        List<JnksIotQueueMsg> msgs = new ArrayList<>(MAX_POLL_SIZE + 1);
        for (int i = 0; i < MAX_POLL_SIZE + 1; i++) {
            DefaultJnksIotQueueMsg msg = gson.fromJson("{\"key\": \"" + UUID.randomUUID() + "\"}", DefaultJnksIotQueueMsg.class);
            msgs.add(msg);
            storage.put(topic, msg);
        }

        assertThat(storage.getLagTotal()).as("total lag is 1001").isEqualTo(MAX_POLL_SIZE + 1);
        assertThat(storage.get(topic)).as("poll exactly 1000 msgs").isEqualTo(msgs.subList(0, MAX_POLL_SIZE));
        assertThat(storage.get(topic)).as("poll last 1 message").isEqualTo(msgs.subList(MAX_POLL_SIZE, MAX_POLL_SIZE + 1));
        assertThat(storage.getLagTotal()).as("total lag is zero").isEqualTo(0);
    }

    private void testPollOnce(final int msgCount) throws InterruptedException {
        List<JnksIotQueueMsg> msgs = new ArrayList<>(msgCount);
        for (int i = 0; i < msgCount; i++) {
            DefaultJnksIotQueueMsg msg = gson.fromJson("{\"key\": \"" + UUID.randomUUID() + "\"}", DefaultJnksIotQueueMsg.class);
            msgs.add(msg);
            storage.put(topic, msg);
        }

        assertThat(storage.getLagTotal()).as("total lag before poll").isEqualTo(msgCount);
        assertThat(storage.get(topic)).as("polled exactly msgs").isEqualTo(msgs.subList(0, msgCount));
        assertThat(storage.getLagTotal()).as("final lag is zero").isEqualTo(0);
    }

    @Test
    public void givenQueueWithExactBatchSize_whenPoll_thenReturnExactBatchSizeList() throws InterruptedException {
        testPollOnce(MAX_POLL_SIZE);
    }

    @Test
    public void givenQueueWithExactBatchSizeMinusOne_whenPoll_thenReturnCorrectSizeList() throws InterruptedException {
        testPollOnce(MAX_POLL_SIZE - 1);
    }

    @Test
    public void givenQueueWithExactBatchSizeMinusTen_whenPoll_thenReturnCorrectSizeList() throws InterruptedException {
        testPollOnce(MAX_POLL_SIZE - 10);
    }

    @Test
    public void givenQueueEmpty_whenPoll_thenReturnEmptyList() throws InterruptedException {
        testPollOnce(0);
    }

    @Test
    public void givenQueueWithSingleMessage_whenPoll_thenReturnSingletonList() throws InterruptedException {
        testPollOnce(1);
    }

    @Test
    public void givenQueueWithTwoMessages_whenPoll_thenReturnCorrectSizeList() throws InterruptedException {
        testPollOnce(2);
    }

    @Test
    public void givenQueueWithTenMessages_whenPoll_thenReturnCorrectSizeList() throws InterruptedException {
        testPollOnce(10);
    }

}
