package com.jnks.iot.server.queue.provider;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToHousekeeperServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineNotificationMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToTransportMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToUsageStatsServiceMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToVersionControlServiceMsg;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;

@Service
public class JnksIotVersionControlProducerProvider implements JnksIotQueueProducerProvider {

    // 使用版本控制队列工厂创建实际的生产者实例
    private final JnksIotVersionControlQueueFactory jnksIotQueueProvider;

    // 核心服务通知生产者
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> toJnksIotCoreNotifications;

    // 使用统计服务生产者
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> toUsageStats;

    // 管家服务生产者
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> toHousekeeper;

    public JnksIotVersionControlProducerProvider(JnksIotVersionControlQueueFactory jnksIotQueueProvider) {
        this.jnksIotQueueProvider = jnksIotQueueProvider;
    }

    @PostConstruct
    public void init() {
        this.toJnksIotCoreNotifications = jnksIotQueueProvider.createJnksIotCoreNotificationsMsgProducer();
        this.toUsageStats = jnksIotQueueProvider.createToUsageStatsServiceMsgProducer();
        this.toHousekeeper = jnksIotQueueProvider.createHousekeeperMsgProducer();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToTransportMsg>> getTransportNotificationsMsgProducer() {
        throw new RuntimeException("Not Implemented! Should not be used by Version Control Service!");
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> getRuleEngineMsgProducer() {
        throw new RuntimeException("Not Implemented! Should not be used by Version Control Service!");
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreMsg>> getJnksIotCoreMsgProducer() {
        throw new RuntimeException("Not Implemented! Should not be used by Version Control Service!");
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> getRuleEngineNotificationsMsgProducer() {
        throw new RuntimeException("Not Implemented! Should not be used by Version Control Service!");
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> getJnksIotCoreNotificationsMsgProducer() {
        return toJnksIotCoreNotifications;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToVersionControlServiceMsg>> getJnksIotVersionControlMsgProducer() {
        throw new RuntimeException("Not Implemented! Should not be used by Version Control Service!");
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> getJnksIotUsageStatsMsgProducer() {
        return toUsageStats;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> getHousekeeperMsgProducer() {
        return toHousekeeper;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToCalculatedFieldMsg>> getCalculatedFieldsMsgProducer() {
        throw new RuntimeException("Not Implemented! Should not be used by Version Control Service!");
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToCalculatedFieldNotificationMsg>> getCalculatedFieldsNotificationsMsgProducer() {
        throw new RuntimeException("Not Implemented! Should not be used by Version Control Service!");
    }

}
