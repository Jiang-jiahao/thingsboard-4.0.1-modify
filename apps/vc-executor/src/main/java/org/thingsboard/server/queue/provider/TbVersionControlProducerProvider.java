package org.thingsboard.server.queue.provider;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.ToCoreMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToCoreNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToHousekeeperServiceMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToRuleEngineNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToTransportMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToUsageStatsServiceMsg;
import org.thingsboard.server.gen.transport.TransportProtos.ToVersionControlServiceMsg;
import org.thingsboard.server.queue.TbQueueProducer;
import org.thingsboard.server.queue.common.TbProtoQueueMsg;

@Service
public class TbVersionControlProducerProvider implements TbQueueProducerProvider {

    // 使用版本控制队列工厂创建实际的生产者实例
    private final TbVersionControlQueueFactory tbQueueProvider;

    // 核心服务通知生产者
    private TbQueueProducer<TbProtoQueueMsg<ToCoreNotificationMsg>> toTbCoreNotifications;

    // 使用统计服务生产者
    private TbQueueProducer<TbProtoQueueMsg<ToUsageStatsServiceMsg>> toUsageStats;

    // 管家服务生产者
    private TbQueueProducer<TbProtoQueueMsg<ToHousekeeperServiceMsg>> toHousekeeper;

    public TbVersionControlProducerProvider(TbVersionControlQueueFactory tbQueueProvider) {
        this.tbQueueProvider = tbQueueProvider;
    }

    @PostConstruct
    public void init() {
        this.toTbCoreNotifications = tbQueueProvider.createTbCoreNotificationsMsgProducer();
        this.toUsageStats = tbQueueProvider.createToUsageStatsServiceMsgProducer();
        this.toHousekeeper = tbQueueProvider.createHousekeeperMsgProducer();
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToTransportMsg>> getTransportNotificationsMsgProducer() {
        throw new RuntimeException("Not Implemented! Should not be used by Version Control Service!");
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToRuleEngineMsg>> getRuleEngineMsgProducer() {
        throw new RuntimeException("Not Implemented! Should not be used by Version Control Service!");
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToCoreMsg>> getTbCoreMsgProducer() {
        throw new RuntimeException("Not Implemented! Should not be used by Version Control Service!");
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToRuleEngineNotificationMsg>> getRuleEngineNotificationsMsgProducer() {
        throw new RuntimeException("Not Implemented! Should not be used by Version Control Service!");
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToCoreNotificationMsg>> getTbCoreNotificationsMsgProducer() {
        return toTbCoreNotifications;
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToVersionControlServiceMsg>> getTbVersionControlMsgProducer() {
        throw new RuntimeException("Not Implemented! Should not be used by Version Control Service!");
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToUsageStatsServiceMsg>> getTbUsageStatsMsgProducer() {
        return toUsageStats;
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<ToHousekeeperServiceMsg>> getHousekeeperMsgProducer() {
        return toHousekeeper;
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<TransportProtos.ToCalculatedFieldMsg>> getCalculatedFieldsMsgProducer() {
        throw new RuntimeException("Not Implemented! Should not be used by Version Control Service!");
    }

    @Override
    public TbQueueProducer<TbProtoQueueMsg<TransportProtos.ToCalculatedFieldNotificationMsg>> getCalculatedFieldsNotificationsMsgProducer() {
        throw new RuntimeException("Not Implemented! Should not be used by Version Control Service!");
    }

}
