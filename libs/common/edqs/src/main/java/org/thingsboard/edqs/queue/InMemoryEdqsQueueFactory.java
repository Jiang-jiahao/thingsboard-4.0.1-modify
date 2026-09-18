package org.thingsboard.edqs.queue;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.server.common.stats.StatsFactory;
import org.thingsboard.server.common.stats.StatsType;
import org.thingsboard.server.gen.transport.TransportProtos.FromEdqsMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToEdqsMsg;
import org.thingsboard.server.queue.TbQueueAdmin;
import org.thingsboard.server.queue.edqs.EdqsConfig;
import org.thingsboard.server.queue.edqs.EdqsQueueFactory;
import org.thingsboard.server.queue.TbQueueConsumer;
import org.thingsboard.server.queue.TbQueueProducer;
import org.thingsboard.server.queue.TbQueueResponseTemplate;
import org.thingsboard.server.queue.common.DefaultTbQueueResponseTemplate;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;
import org.thingsboard.server.queue.memory.InMemoryStorage;
import org.thingsboard.server.queue.memory.InMemoryTbQueueConsumer;
import org.thingsboard.server.queue.memory.InMemoryTbQueueProducer;

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
                .executor(ThingsBoardExecutors.newWorkStealingPool(5, "edqs"))
                .build();
    }

    @Override
    public TbQueueAdmin getEdqsQueueAdmin() {
        return queueAdmin;
    }

}
