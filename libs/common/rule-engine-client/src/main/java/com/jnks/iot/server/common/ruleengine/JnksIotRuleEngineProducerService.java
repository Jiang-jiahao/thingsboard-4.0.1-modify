package com.jnks.iot.server.common.ruleengine;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.common.msg.queue.TopicPartitionInfo;
import com.jnks.iot.server.gen.transport.TransportProtos.ToRuleEngineMsg;
import com.jnks.iot.server.queue.JnksIotQueueCallback;
import com.jnks.iot.server.queue.JnksIotQueueProducer;
import com.jnks.iot.server.queue.common.JnksIotProtoQueueMsg;
import com.jnks.iot.server.queue.discovery.PartitionService;

import java.util.List;
import java.util.UUID;

/**
 * 对ruleEngineMsgProducer的包装
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class JnksIotRuleEngineProducerService {

    private final PartitionService partitionService;

    public void sendToRuleEngine(JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> producer,
                                 TenantId tenantId, JnksIotMsg jnksIotMsg, JnksIotQueueCallback callback) {
        List<TopicPartitionInfo> tpis = partitionService.resolveAll(ServiceType.JNKS_IOT_RULE_ENGINE, jnksIotMsg.getQueueName(), tenantId, jnksIotMsg.getOriginator());
        if (tpis.size() > 1) {
            UUID correlationId = UUID.randomUUID();
            for (int i = 0; i < tpis.size(); i++) {
                TopicPartitionInfo tpi = tpis.get(i);
                Integer partition = tpi.getPartition().orElse(null);
                UUID id = i > 0 ? UUID.randomUUID() : jnksIotMsg.getId();

                jnksIotMsg = jnksIotMsg.transform()
                        .id(id)
                        .correlationId(correlationId)
                        .partition(partition)
                        .build();
                sendToRuleEngine(producer, tpi, tenantId, jnksIotMsg, i == tpis.size() - 1 ? callback : null);
            }
        } else {
            sendToRuleEngine(producer, tpis.get(0), tenantId, jnksIotMsg, callback);
        }
    }

    private void sendToRuleEngine(JnksIotQueueProducer<JnksIotProtoQueueMsg<ToRuleEngineMsg>> producer, TopicPartitionInfo tpi,
                                  TenantId tenantId, JnksIotMsg jnksIotMsg, JnksIotQueueCallback callback) {
        if (log.isTraceEnabled()) {
            log.trace("[{}][{}] Pushing to topic {} message {}", tenantId, jnksIotMsg.getOriginator(), tpi.getFullTopicName(), jnksIotMsg);
        }
        ToRuleEngineMsg msg = ToRuleEngineMsg.newBuilder()
                .setJnksIotMsg(JnksIotMsg.toByteString(jnksIotMsg))
                .setTenantIdMSB(tenantId.getId().getMostSignificantBits())
                .setTenantIdLSB(tenantId.getId().getLeastSignificantBits()).build();
        producer.send(tpi, new JnksIotProtoQueueMsg<>(jnksIotMsg.getId(), msg), callback);
    }

}
