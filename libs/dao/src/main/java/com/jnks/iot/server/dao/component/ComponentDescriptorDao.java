package com.jnks.iot.server.dao.component;

import com.jnks.iot.server.common.data.id.ComponentDescriptorId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.plugin.ComponentDescriptor;
import com.jnks.iot.server.common.data.plugin.ComponentScope;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.dao.Dao;

import java.util.Optional;

/**
 * @author Andrew Shvayka
 */
public interface ComponentDescriptorDao extends Dao<ComponentDescriptor> {

    Optional<ComponentDescriptor> saveIfNotExist(TenantId tenantId, ComponentDescriptor component);

    ComponentDescriptor findById(TenantId tenantId, ComponentDescriptorId componentId);

    ComponentDescriptor findByClazz(TenantId tenantId, String clazz);

    PageData<ComponentDescriptor> findByTypeAndPageLink(TenantId tenantId, ComponentType type, PageLink pageLink);

    PageData<ComponentDescriptor> findByScopeAndTypeAndPageLink(TenantId tenantId, ComponentScope scope, ComponentType type, PageLink pageLink);

    void deleteById(TenantId tenantId, ComponentDescriptorId componentId);

    void deleteByClazz(TenantId tenantId, String clazz);

}
