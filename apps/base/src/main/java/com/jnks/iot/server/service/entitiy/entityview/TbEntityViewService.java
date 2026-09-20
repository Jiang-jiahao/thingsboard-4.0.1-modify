package com.jnks.iot.server.service.entitiy.entityview;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.EntityView;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.EntityViewId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.plugin.ComponentLifecycleListener;

import java.util.List;

public interface TbEntityViewService extends ComponentLifecycleListener {

    EntityView save(EntityView entityView, EntityView existingEntityView, User user) throws Exception;

    void updateEntityViewAttributes(TenantId tenantId, EntityView savedEntityView, EntityView oldEntityView, User user) throws JnksIotException;

    void delete(EntityView entity, User user) throws JnksIotException;

    EntityView assignEntityViewToCustomer(TenantId tenantId, EntityViewId entityViewId, Customer customer, User user) throws JnksIotException;

    EntityView assignEntityViewToPublicCustomer(TenantId tenantId, EntityViewId entityViewId, User user) throws JnksIotException;

    EntityView unassignEntityViewFromCustomer(TenantId tenantId, EntityViewId entityViewId, Customer customer, User user) throws JnksIotException;

    ListenableFuture<List<EntityView>> findEntityViewsByTenantIdAndEntityIdAsync(TenantId tenantId, EntityId entityId);
}
