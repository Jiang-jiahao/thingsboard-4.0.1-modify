package com.jnks.iot.edqs.queue;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.JnksIotExecutors;
import com.jnks.iot.server.common.stats.StatsFactory;
import com.jnks.iot.server.common.stats.StatsType;
import com.jnks.iot.server.gen.transport.TransportProtos.FromEdqsMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToEdqsMsg;
import com.jnks.iot.server.queue.TbQueueAdmin;
import com.jnks.iot.server.queue.edqs.EdqsConfig;
import com.jnks.iot.server.queue.edqs.EdqsQueueFactory;
import com.jnks.iot.server.queue.TbQueueConsumer;
import com.jnks.iot.server.queue.TbQueueProducer;
import com.jnks.iot.server.queue.TbQueueResponseTemplate;
import com.jnks.iot.server.queue.common.DefaultTbQueueResponseTemplate;
import com.jnks.iot.server.queue.common.TbProtoQueueMsg;
import com.jnks.iot.server.queue.memory.InMemoryStorage;
import com.jnks.iot.server.queue.memory.InMemoryTbQueueConsumer;
import com.jnks.iot.server.queue.memory.InMemoryTbQueueProducer;

@Component
@ConditionalOnExpression("'${queue.edqs.sync.enabled:true}'=='true' && '${queue.edqs.mode:null}'=='local' && '${queue.type:null}'=='in-memory'")
@RequiredArgsConstructor
public class InMemoryEdqsQueueFactory implements EdqsQueueFactory {

    private final InMemoryStorage storage;
    private final EdqsConfig edqsConfig;
    private final StatsFactory statsFactory;
    private final TbQueueAdmin queueAdmin;

    @Override
    public TbQueueConsumer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsEventsConsumer() {
        return new InMemoryTbQueueConsumer<>(storage, edqsConfig.getEventsTopic());
    }

    @Override
    public TbQueueConsumer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsEventsToBackupConsumer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TbQueueConsumer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsStateConsumer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToEdqsMsg>> createEdqsStateProducer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TbQueueResponseTemplate<TbProtoQueueMsg<ToEdqsMsg>, TbProtoQueueMsg<FromEdqsMsg>> createEdqsResponseTemplate() {
        TbQueueConsumer<TbProtoQueueMsg<ToEdqsMsg>> requestConsumer = new InMemoryTbQueueConsumer<>(storage, edqsConfig.getRequestsTopic());
        TbQueueProducer<TbProtoQueueMsg<FromEdqsMsg>> responseProducer = new InMemoryTbQueueProducer<>(storage, edqsConfig.getResponsesTopic());
        return DefaultTbQueueResponseTemplate.<TbProtoQueueMsg<ToEdqsMsg>, TbProtoQueueMsg<FromEdqsMsg>>builder()
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
    public TbQueueAdmin getEdqsQueueAdmin() {
        return queueAdmin;
    }

}
