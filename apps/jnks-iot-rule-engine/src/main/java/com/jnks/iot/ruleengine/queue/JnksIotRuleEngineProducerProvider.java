package com.jnks.iot.ruleengine.queue;

import com.jnks.iot.server.queue.provider.*;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;
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

@Service
public class JnksIotRuleEngineProducerProvider implements JnksIotQueueProducerProvider {

    // 使用规则引擎队列工厂创建实际的生产者实例
    private final JnksIotRuleEngineQueueFactory jnksIotQueueProvider;

    // 设备传输层消息生产者
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToTransportMsg>> toTransport;

    // 规则引擎消息生产者
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> toRuleEngine;

    // 核心服务消息生产者
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreMsg>> toJnksIotCore;

    // 规则引擎通知生产者
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineNotificationMsg>> toRuleEngineNotifications;

    // 核心服务通知生产者
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCoreNotificationMsg>> toJnksIotCoreNotifications;

    // 使用统计服务生产者
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToUsageStatsServiceMsg>> toUsageStats;

    // 管家服务生产者
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> toHousekeeper;

    // 计算字段服务生产者
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldMsg>> toCalculatedFields;

    // 计算字段通知生产者
    private JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> toCalculatedFieldNotifications;

    public JnksIotRuleEngineProducerProvider(JnksIotRuleEngineQueueFactory jnksIotQueueProvider) {
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
        throw new RuntimeException("Not Implemented! Should not be used by Rule Engine!");
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToHousekeeperServiceMsg>> getHousekeeperMsgProducer() {
        return toHousekeeper;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldMsg>> getCalculatedFieldsMsgProducer() {
        return toCalculatedFields;
    }

    @Override
    public JnksIotQueueProducer<JnksIotProtoQueueMsg<ToCalculatedFieldNotificationMsg>> getCalculatedFieldsNotificationsMsgProducer() {
        return toCalculatedFieldNotifications;
    }

}
