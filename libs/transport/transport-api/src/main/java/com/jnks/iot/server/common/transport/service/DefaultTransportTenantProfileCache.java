package com.jnks.iot.server.common.transport.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import com.jnks.iot.server.common.data.ApiUsageState;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.TenantProfile;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.TenantProfileId;
import com.jnks.iot.server.common.transport.TransportService;
import com.jnks.iot.server.common.transport.TransportTenantProfileCache;
import com.jnks.iot.server.common.transport.limits.TransportRateLimitService;
import com.jnks.iot.server.common.transport.profile.TenantProfileUpdateResult;
import com.jnks.iot.server.common.util.ProtoUtils;
import com.jnks.iot.server.gen.transport.TransportProtos;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Component
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true'")
@Slf4j
public class DefaultTransportTenantProfileCache implements TransportTenantProfileCache {

    private final Lock tenantProfileFetchLock = new ReentrantLock();
    private final ConcurrentMap<TenantProfileId, TenantProfile> profiles = new ConcurrentHashMap<>();
    private final ConcurrentMap<TenantId, TenantProfileId> tenantIds = new ConcurrentHashMap<>();
    private final ConcurrentMap<TenantProfileId, Set<TenantId>> tenantProfileIds = new ConcurrentHashMap<>();

    private TransportRateLimitService rateLimitService;
    private TransportService transportService;

    @Lazy
    @Autowired
    public void setRateLimitService(TransportRateLimitService rateLimitService) {
        this.rateLimitService = rateLimitService;
    }

    @Lazy
    @Autowired
    public void setTransportService(TransportService transportService) {
        this.transportService = transportService;
    }

    @Override
    public TenantProfile get(TenantId tenantId) {
        return getTenantProfile(tenantId);
    }

    @Override
    public TenantProfileUpdateResult put(TransportProtos.TenantProfileProto proto) {
        TenantProfile profile = ProtoUtils.fromProto(proto);
        log.trace("[{}] put: {}", profile.getId(), profile);
        profiles.put(profile.getId(), profile);
        Set<TenantId> affectedTenants = tenantProfileIds.get(profile.getId());
        return new TenantProfileUpdateResult(profile, affectedTenants != null ? affectedTenants : Collections.emptySet());
    }

    @Override
    public boolean put(TenantId tenantId, TenantProfileId profileId) {
        log.trace("[{}] put: {}", tenantId, profileId);
        TenantProfileId oldProfileId = tenantIds.get(tenantId);
        if (oldProfileId != null && !oldProfileId.equals(profileId)) {
            tenantProfileIds.computeIfAbsent(oldProfileId, id -> ConcurrentHashMap.newKeySet()).remove(tenantId);
            tenantIds.put(tenantId, profileId);
            tenantProfileIds.computeIfAbsent(profileId, id -> ConcurrentHashMap.newKeySet()).add(tenantId);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Set<TenantId> remove(TenantProfileId profileId) {
        Set<TenantId> tenants = tenantProfileIds.remove(profileId);
        if (tenants != null) {
            tenants.forEach(tenantIds::remove);
        }
        profiles.remove(profileId);
        return tenants;
    }

    private TenantProfile getTenantProfile(TenantId tenantId) {
        TenantProfile profile = null;
        TenantProfileId tenantProfileId = tenantIds.get(tenantId);
        if (tenantProfileId != null) {
            profile = profiles.get(tenantProfileId);
        }
        if (profile == null) {
            tenantProfileFetchLock.lock();
            try {
                tenantProfileId = tenantIds.get(tenantId);
                if (tenantProfileId != null) {
                    profile = profiles.get(tenantProfileId);
                }
                if (profile == null) {
                    TransportProtos.GetEntityProfileRequestMsg msg = TransportProtos.GetEntityProfileRequestMsg.newBuilder()
                            .setEntityType(EntityType.TENANT.name())
                            .setEntityIdMSB(tenantId.getId().getMostSignificantBits())
                            .setEntityIdLSB(tenantId.getId().getLeastSignificantBits())
                            .build();
                    TransportProtos.GetEntityProfileResponseMsg entityProfileMsg = transportService.getEntityProfile(msg);
                    profile = ProtoUtils.fromProto(entityProfileMsg.getTenantProfile());
                    TenantProfile existingProfile = profiles.get(profile.getId());
                    if (existingProfile != null) {
                        profile = existingProfile;
                    } else {
                        profiles.put(profile.getId(), profile);
                    }
                    tenantProfileIds.computeIfAbsent(profile.getId(), id -> ConcurrentHashMap.newKeySet()).add(tenantId);
                    tenantIds.put(tenantId, profile.getId());
                    ApiUsageState apiUsageState = ProtoUtils.fromProto(entityProfileMsg.getApiState());
                    rateLimitService.update(tenantId, apiUsageState.isTransportEnabled());
                }
            } finally {
                tenantProfileFetchLock.unlock();
            }
        }
        return profile;
    }

}
