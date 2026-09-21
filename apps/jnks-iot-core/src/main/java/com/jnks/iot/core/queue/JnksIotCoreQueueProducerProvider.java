package com.jnks.iot.core.queue;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldMsg;
import com.jnks.iot.server.gen.transport.TransportProtos.ToCalculatedFieldNotificationMsg;
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
import com.jnks.iot.server.queue.provider.JnksIotCoreQueueFactory;
import com.jnks.iot.server.queue.provider.JnksIotQueueProducerProvider;

/**
 * Core 进程的队列生产者。单体使用 {@code com.jnks.iot.monolith.queue} 下的对应类。
 */
@Service
public class JnksIotCoreQueueProducerProvider implements JnksIotQueueProducerProvider {

    private final JnksIotCoreQueueFactory jnksIotQueueProvider;

    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToTransportMsg>> toTransport;
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> toRuleEngine;
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreMsg>> toJnksIotCore;
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> toRuleEngineNotifications;
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> toJnksIotCoreNotifications;
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> toUsageStats;
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToVersionControlServiceMsg>> toVersionControl;
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> toHousekeeper;
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldMsg>> toCalculatedFields;
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> toCalculatedFieldNotifications;

    public JnksIotCoreQueueProducerProvider(JnksIotCoreQueueFactory jnksIotQueueProvider) {
        this.jnksIotQueueProvider = jnksIotQueueProvider;
    }

    @PostConstruct
    public void init() {
        this.toJnksIotCore = jnksIotQueueProvider.createJnksIotCoreMsgProducer();
        this.toTransport = jnksIotQueueProvider.createTransportNotificationsMsgProducer();
        this.toRuleEngine = jnksIotQueueProvider.createRuleEngineMsgProducer();
        this.toRuleEngineNotifications = jnksIotQueueProvider.createRuleEngineNotificationsMsgProducer();
        this.toJnksIotCoreNotifications = jnksIotQueueProvider.createJnksIotCoreNotificationsMsgProducer();
        this.toUsageStats = jnksIotQueueProvider.createToUsageStatsServiceMsgProducer();
        this.toVersionControl = jnksIotQueueProvider.createVersionControlMsgProducer();
        this.toHousekeeper = jnksIotQueueProvider.createHousekeeperMsgProducer();
        this.toCalculatedFields = jnksIotQueueProvider.createToCalculatedFieldMsgProducer();
        this.toCalculatedFieldNotifications = jnksIotQueueProvider.createToCalculatedFieldNotificationMsgProducer();
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToTransportMsg>> getTransportNotificationsMsgProducer() {
        return toTransport;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> getRuleEngineMsgProducer() {
        return toRuleEngine;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> getRuleEngineNotificationsMsgProducer() {
        return toRuleEngineNotifications;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreMsg>> getJnksIotCoreMsgProducer() {
        return toJnksIotCore;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> getJnksIotCoreNotificationsMsgProducer() {
        return toJnksIotCoreNotifications;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> getJnksIotUsageStatsMsgProducer() {
        return toUsageStats;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToVersionControlServiceMsg>> getJnksIotVersionControlMsgProducer() {
        return toVersionControl;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> getHousekeeperMsgProducer() {
        return toHousekeeper;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<TransportProtos.ToCalculatedFieldMsg>> getCalculatedFieldsMsgProducer() {
        return toCalculatedFields;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> getCalculatedFieldsNotificationsMsgProducer() {
        return toCalculatedFieldNotifications;
    }

}
