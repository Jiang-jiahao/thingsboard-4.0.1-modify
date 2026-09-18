package org.thingsboard.monolith.queue;

import org.springframework.stereotype.Service;
import org.thingsboard.core.queue.TbCoreQueueProducerProvider;
import org.thingsboard.server.queue.provider.TbCoreQueueFactory;

/**
 * 单体进程的队列生产者。注入 {@link KafkaMonolithQueueFactory} / {@link InMemoryMonolithQueueFactory}
 * （二者都实现 {@link TbCoreQueueFactory}），与 Core 微服务的 Provider 逻辑相同。
 */
@Service
public class TbMonolithQueueProducerProvider extends TbCoreQueueProducerProvider {

    public TbMonolithQueueProducerProvider(TbCoreQueueFactory tbQueueProvider) {
        super(tbQueueProvider);
    }

}
