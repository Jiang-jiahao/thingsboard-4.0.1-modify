package org.thingsboard.server.dao.sql.query;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.ObjectType;
import org.thingsboard.server.common.data.edqs.EdqsObject;
import org.thingsboard.server.common.data.edqs.ToCoreEdqsMsg;
import org.thingsboard.server.common.data.edqs.ToCoreEdqsRequest;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.msg.edqs.EdqsService;

/**
 * {@link EdqsService} 的空实现：容器中尚无真正的 EDQS 写路径服务时使用。
 * <p>
 * 属性/时序等 DAO 在落库后会回调 {@link EdqsService#onUpdate}/{@link EdqsService#onDelete}；
 * 若未部署 {@code DefaultEdqsService}（通常在 application/edqs），则本类作为默认 Bean，
 * 方法均为空操作，不影响主流程落库。
 * <p>
 * {@link ConditionalOnMissingBean}：存在真实现时不注册本类。
 * 放在 dao 是为了让“只用 dao、未挂 EDQS 模块”的进程也能启动。
 */
@Service
@ConditionalOnMissingBean(value = EdqsService.class, ignored = DummyEdqsService.class)
public class DummyEdqsService implements EdqsService {

    @Override
    public void onUpdate(TenantId tenantId, EntityId entityId, Object entity) {}

    @Override
    public void onUpdate(TenantId tenantId, ObjectType objectType, EdqsObject object) {}

    @Override
    public void onDelete(TenantId tenantId, EntityId entityId) {}

    @Override
    public void onDelete(TenantId tenantId, ObjectType objectType, EdqsObject object) {}

    @Override
    public void processSystemRequest(ToCoreEdqsRequest request) {}

    @Override
    public void processSystemMsg(ToCoreEdqsMsg request) {}

}
