package com.jnks.iot.monolith.queue;

import org.springframework.stereotype.Service;
import com.jnks.iot.core.queue.JnksIotCoreQueueProducerProvider;
import com.jnks.iot.server.queue.provider.JnksIotCoreQueueFactory;

/**
 * 单体进程的队列生产者。注入 {@link KafkaMonolithQueueFactory} / {@link InMemoryMonolithQueueFactory}
 * （二者都实现 {@link JnksIotCoreQueueFactory}），与 Core 微服务的 Provider 逻辑相同。
 */
@Service
public class JnksIotMonolithQueueProducerProvider extends JnksIotCoreQueueProducerProvider {

    public JnksIotMonolithQueueProducerProvider(JnksIotCoreQueueFactory jnksIotQueueProvider) {
        super(jnksIotQueueProvider);
    }

}
