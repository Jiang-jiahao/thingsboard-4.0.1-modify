package com.jnks.iot.edqs.queue;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.JnksIotExecutors;
import com.jnks.iot.server.common.stats.StatsFactory;
import com.jnks.iot.server.common.stats.StatsType;
import com.jnks.iot.server.gen.transport.TransportProtos.FromEdqsMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToEdqsMsg;
import com.jnks.iot.server.queue.JnksIotQueueAdmin;
import com.jnks.iot.server.queue.edqs.EdqsConfig;
import com.jnks.iot.server.queue.edqs.EdqsQueueFactory;
import com.jnks.iot.server.queue.JnksIotQueueConsumer;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.JnksIotQueueResponseTemplate;
import com.jnks.iot.server.queue.common.DefaultJnksIotQueueResponseTemplate;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.memory.InMemoryStorage;
import com.jnks.iot.server.queue.memory.InMemoryJnksIotQueueConsumer;
import com.jnks.iot.server.queue.memory.InMemoryJnksIotQueueProducer;

@Component
@ConditionalOnExpression("'${queue.edqs.sync.enabled:true}'=='true' && '${queue.edqs.mode:null}'=='local' && '${queue.type:null}'=='in-memory'")
@RequiredArgsConstructor
public class InMemoryEdqsQueueFactory implements EdqsQueueFactory {

    private final InMemoryStorage storage;
    private final EdqsConfig edqsConfig;
    private final StatsFactory statsFactory;
    private final JnksIotQueueAdmin queueAdmin;

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsEventsConsumer() {
        return new InMemoryJnksIotQueueConsumer<>(storage, edqsConfig.getEventsTopic());
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsEventsToBackupConsumer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsStateConsumer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToEdqsMsg>> createEdqsStateProducer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JnksIotQueueResponseTemplate<JnksIotProtoQueueMsg<ToEdqsMsg>, JnksIotProtoQueueMsg<FromEdqsMsg>> createEdqsResponseTemplate() {
        JnksIotQueueConsumer<JnksIotProtoQueueMsg<ToEdqsMsg>> requestConsumer = new InMemoryJnksIotQueueConsumer<>(storage, edqsConfig.getRequestsTopic());
        JnksIotQueueProducer<JnksIotProtoQueueMsg<FromEdqsMsg>> responseProducer = new InMemoryJnksIotQueueProducer<>(storage, edqsConfig.getResponsesTopic());
        return DefaultJnksIotQueueResponseTemplate.<JnksIotProtoQueueMsg<ToEdqsMsg>, JnksIotProtoQueueMsg<FromEdqsMsg>>builder()
                .requestTemplate(requestConsumer)
                .responseTemplate(responseProducer)
                .maxPendingRequests(edqsConfig.getMaxPendingRequests())
                .requestTimeout(edqsConfig.getMaxRequestTimeout())
                .pollInterval(edqsConfig.getPollInterval())
                .stats(statsFactory.createMessagesStats(StatsType.EDQS.getName()))
                .executor(JnksIotExecutors.newWorkStealingPool(5, "edqs"))
                .build();
    }

    @Override
    public JnksIotQueueAdmin getEdqsQueueAdmin() {
        return queueAdmin;
    }

}
