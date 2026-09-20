package com.jnks.iot.server.actors.stats;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.TbActor;
import com.jnks.iot.server.actors.TbActorId;
import com.jnks.iot.server.actors.TbStringActorId;
import com.jnks.iot.server.actors.service.ContextAwareActor;
import com.jnks.iot.server.actors.service.ContextBasedCreator;
import com.jnks.iot.server.common.data.event.StatisticsEvent;
import com.jnks.iot.server.common.msg.MsgType;
import com.jnks.iot.server.common.msg.TbActorMsg;

@Slf4j
public class StatsActor extends ContextAwareActor {

    public StatsActor(ActorSystemContext context) {
        super(context);
    }

    @Override
    protected boolean doProcess(TbActorMsg msg) {
        log.debug("Received message: {}", msg);
        if (msg.getMsgType().equals(MsgType.STATS_PERSIST_MSG)) {
            onStatsPersistMsg((StatsPersistMsg) msg);
            return true;
        } else {
            return false;
        }
    }

    public void onStatsPersistMsg(StatsPersistMsg msg) {
        if (msg.isEmpty()) {
            return;
        }
        systemContext.getEventService().saveAsync(StatisticsEvent.builder()
                .tenantId(msg.getTenantId())
                .entityId(msg.getEntityId().getId())
                .serviceId(systemContext.getServiceInfoProvider().getServiceId())
                .messagesProcessed(msg.getMessagesProcessed())
                .errorsOccurred(msg.getErrorsOccurred())
                .build()
        );
    }

    private JsonNode toBodyJson(String serviceId, long messagesProcessed, long errorsOccurred) {
        return JacksonUtil.newObjectNode().put("server", serviceId).put("messagesProcessed", messagesProcessed).put("errorsOccurred", errorsOccurred);
    }

    public static class ActorCreator extends ContextBasedCreator {
        private final String actorId;

        public ActorCreator(ActorSystemContext context, String actorId) {
            super(context);
            this.actorId = actorId;
        }

        @Override
        public TbActorId createActorId() {
            return new TbStringActorId(actorId);
        }

        @Override
        public TbActor createActor() {
            return new StatsActor(context);
        }
    }
}
