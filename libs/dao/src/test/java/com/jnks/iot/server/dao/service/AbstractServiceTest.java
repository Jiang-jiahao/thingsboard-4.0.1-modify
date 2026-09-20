package com.jnks.iot.server.dao.service;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.DeviceProfileType;
import com.jnks.iot.server.common.data.DeviceTransportType;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.OtaPackage;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.asset.AssetProfile;
import com.jnks.iot.server.common.data.device.profile.DefaultDeviceProfileConfiguration;
import com.jnks.iot.server.common.data.device.profile.DefaultDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileData;
import com.jnks.iot.server.common.data.event.RuleNodeDebugEvent;
import com.jnks.iot.server.common.data.housekeeper.HousekeeperTaskType;
import com.jnks.iot.server.common.data.housekeeper.TenantEntitiesDeletionHousekeeperTask;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.HasId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.oauth2.MapperType;
import com.jnks.iot.server.common.data.oauth2.OAuth2Client;
import com.jnks.iot.server.common.data.oauth2.OAuth2CustomMapperConfig;
import com.jnks.iot.server.common.data.oauth2.OAuth2MapperConfig;
import com.jnks.iot.server.common.data.oauth2.PlatformType;
import com.jnks.iot.server.common.data.ota.ChecksumAlgorithm;
import com.jnks.iot.server.common.data.ota.OtaPackageType;
import com.jnks.iot.server.common.msg.housekeeper.HousekeeperClient;
import com.jnks.iot.server.dao.audit.AuditLogLevelFilter;
import com.jnks.iot.server.dao.audit.AuditLogLevelMask;
import com.jnks.iot.server.dao.audit.AuditLogLevelProperties;
import com.jnks.iot.server.dao.entity.EntityDaoService;
import com.jnks.iot.server.dao.entity.EntityServiceRegistry;
import com.jnks.iot.server.dao.tenant.TenantService;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = AbstractServiceTest.class, loader = AnnotationConfigContextLoader.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Configuration
@ComponentScan("com.jnks.iot.server")
public abstract class AbstractServiceTest {

    public static final TenantId SYSTEM_TENANT_ID = TenantId.SYS_TENANT_ID;

    @Autowired
    protected TenantService tenantService;

    @Autowired
    protected EntityServiceRegistry entityServiceRegistry;

    protected TenantId tenantId;

    @Before
    public void beforeAbstractService() {
        tenantId = createTenant().getId();
    }

    @After
    public void afterAbstractService() {
        tenantService.deleteTenants();
    }

    public class IdComparator<D extends HasId> implements Comparator<D> {
        @Override
        public int compare(D o1, D o2) {
            return o1.getId().getId().compareTo(o2.getId().getId());
        }
    }


    protected RuleNodeDebugEvent generateEvent(TenantId tenantId, EntityId entityId) throws IOException {
        return generateEvent(tenantId, entityId, null);
    }

    protected RuleNodeDebugEvent generateEvent(TenantId tenantId, EntityId entityId, String eventType) throws IOException {
        if (tenantId == null) {
            tenantId = TenantId.fromUUID(Uuids.timeBased());
        }
        return RuleNodeDebugEvent.builder()
                .tenantId(tenantId)
                .entityId(entityId.getId())
                .serviceId("server A")
                .eventType(eventType)
                .data(JacksonUtil.toString(readFromResource("TestJsonData.json")))
                .build();
    }

    public JsonNode readFromResource(String resourceName) throws IOException {
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(resourceName)) {
            return JacksonUtil.fromBytes(Objects.requireNonNull(is).readAllBytes());
        }
    }

    @Bean
    public AuditLogLevelFilter auditLogLevelFilter() {
        Map<String, String> mask = new HashMap<>();
        for (EntityType entityType : EntityType.values()) {
            mask.put(entityType.name().toLowerCase(), AuditLogLevelMask.RW.name());
        }
        var props = new AuditLogLevelProperties();
        props.setMask(mask);
        return new AuditLogLevelFilter(props);
    }

    @Bean
    public HousekeeperClient housekeeperClient() {
        return task -> {
            if (task.getTaskType() == HousekeeperTaskType.DELETE_TENANT_ENTITIES) {
                EntityDaoService entityService = entityServiceRegistry.getServiceByEntityType(((TenantEntitiesDeletionHousekeeperTask) task).getEntityType());
                entityService.deleteByTenantId(task.getTenantId());
            }
        };
    }

    protected DeviceProfile createDeviceProfile(TenantId tenantId, String name) {
        DeviceProfile deviceProfile = new DeviceProfile();
        deviceProfile.setTenantId(tenantId);
        deviceProfile.setName(name);
        deviceProfile.setType(DeviceProfileType.DEFAULT);
        deviceProfile.setTransportType(DeviceTransportType.DEFAULT);
        deviceProfile.setDescription(name + " Test");
        DeviceProfileData deviceProfileData = new DeviceProfileData();
        DefaultDeviceProfileConfiguration configuration = new DefaultDeviceProfileConfiguration();
        DefaultDeviceProfileTransportConfiguration transportConfiguration = new DefaultDeviceProfileTransportConfiguration();
        deviceProfileData.setConfiguration(configuration);
        deviceProfileData.setTransportConfiguration(transportConfiguration);
        deviceProfile.setProfileData(deviceProfileData);
        deviceProfile.setDefault(false);
        deviceProfile.setDefaultRuleChainId(null);
        return deviceProfile;
    }

    protected AssetProfile createAssetProfile(TenantId tenantId, String name) {
        AssetProfile assetProfile = new AssetProfile();
        assetProfile.setTenantId(tenantId);
        assetProfile.setName(name);
        assetProfile.setDescription(name + " Test");
        assetProfile.setDefault(false);
        assetProfile.setDefaultRuleChainId(null);
        return assetProfile;
    }

    public Tenant createTenant() {
        Tenant tenant = new Tenant();
        tenant.setTitle("My tenant " + UUID.randomUUID());
        Tenant savedTenant = tenantService.saveTenant(tenant);
        assertNotNull(savedTenant);
        return savedTenant;
    }

    protected OtaPackage constructDefaultOtaPackage(TenantId tenantId, DeviceProfileId deviceProfileId) {
        OtaPackage firmware = new OtaPackage();
        firmware.setTenantId(tenantId);
        firmware.setDeviceProfileId(deviceProfileId);
        firmware.setType(OtaPackageType.FIRMWARE);
        firmware.setTitle("My firmware");
        firmware.setVersion("3.3.3");
        firmware.setFileName("filename.txt");
        firmware.setContentType("text/plain");
        firmware.setChecksumAlgorithm(ChecksumAlgorithm.SHA256);
        firmware.setChecksum("4bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a");
        firmware.setData(ByteBuffer.wrap(new byte[]{1}));
        firmware.setDataSize(1L);
        return firmware;
    }

    protected OAuth2Client validClientInfo(TenantId tenantId, String title) {
        return validClientInfo(tenantId, title, null);
    }

    protected OAuth2Client validClientInfo(TenantId tenantId, String title, List<PlatformType> platforms) {
        OAuth2Client oAuth2Client = new OAuth2Client();
        oAuth2Client.setTenantId(tenantId);
        oAuth2Client.setTitle(title);
        oAuth2Client.setClientId(UUID.randomUUID().toString());
        oAuth2Client.setClientSecret(UUID.randomUUID().toString());
        oAuth2Client.setAuthorizationUri(UUID.randomUUID().toString());
        oAuth2Client.setAccessTokenUri(UUID.randomUUID().toString());
        oAuth2Client.setScope(Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        oAuth2Client.setPlatforms(platforms == null ? Collections.emptyList() : platforms);
        oAuth2Client.setUserInfoUri(UUID.randomUUID().toString());
        oAuth2Client.setUserNameAttributeName(UUID.randomUUID().toString());
        oAuth2Client.setJwkSetUri(UUID.randomUUID().toString());
        oAuth2Client.setClientAuthenticationMethod(UUID.randomUUID().toString());
        oAuth2Client.setLoginButtonLabel(UUID.randomUUID().toString());
        oAuth2Client.setLoginButtonIcon(UUID.randomUUID().toString());
        oAuth2Client.setAdditionalInfo(JacksonUtil.newObjectNode().put(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        oAuth2Client.setMapperConfig(
                OAuth2MapperConfig.builder()
                        .allowUserCreation(true)
                        .activateUser(true)
                        .type(MapperType.CUSTOM)
                        .custom(
                                OAuth2CustomMapperConfig.builder()
                                        .url(UUID.randomUUID().toString())
                                        .build()
                        )
                        .build());
        return oAuth2Client;
    }

}
