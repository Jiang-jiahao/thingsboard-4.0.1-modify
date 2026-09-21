package com.jnks.iot.server.service.subscription;

import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.data.query.EntityCountQuery;
import com.jnks.iot.server.dao.attributes.AttributesService;
import com.jnks.iot.server.dao.entity.EntityService;
import com.jnks.iot.server.service.ws.WebSocketService;
import com.jnks.iot.server.service.ws.WebSocketSessionRef;
import com.jnks.iot.server.service.ws.telemetry.cmd.v2.EntityCountUpdate;

@Slf4j
public class JnksIotEntityCountSubCtx extends JnksIotAbstractEntityQuerySubCtx<EntityCountQuery> {

    private volatile int result;

    public JnksIotEntityCountSubCtx(String serviceId, WebSocketService wsService, EntityService entityService,
                               JnksIotLocalSubscriptionService localSubscriptionService, AttributesService attributesService,
                               SubscriptionServiceStatistics stats, WebSocketSessionRef sessionRef, int cmdId) {
        super(serviceId, wsService, entityService, localSubscriptionService, attributesService, stats, sessionRef, cmdId);
    }

    @Override
    public void fetchData() {
        result = (int) entityService.countEntitiesByQuery(getTenantId(), getCustomerId(), query);
        sendWsMsg(new EntityCountUpdate(cmdId, result));
    }

    @Override
    protected void update() {
        int newCount = (int) entityService.countEntitiesByQuery(getTenantId(), getCustomerId(), query);
        if (newCount != result) {
            result = newCount;
            sendWsMsg(new EntityCountUpdate(cmdId, result));
        }
    }

    @Override
    public boolean isDynamic() {
        return true;
    }
}
