package com.jnks.iot.server.service.device;

import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.DeviceProfileProvisionType;
import com.jnks.iot.server.common.data.DeviceProfileType;
import com.jnks.iot.server.common.data.DeviceTransportType;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.device.credentials.BasicMqttCredentials;
import com.jnks.iot.server.common.data.device.credentials.lwm2m.LwM2MClientCredential;
import com.jnks.iot.server.common.data.device.credentials.lwm2m.LwM2MSecurityMode;
import com.jnks.iot.server.common.data.device.data.DeviceData;
import com.jnks.iot.server.common.data.device.data.PowerMode;
import com.jnks.iot.server.common.data.device.data.SnmpDeviceTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.DefaultDeviceProfileConfiguration;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileData;
import com.jnks.iot.server.common.data.device.profile.DisabledDeviceProfileProvisionConfiguration;
import com.jnks.iot.server.common.data.device.profile.Lwm2mDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.lwm2m.OtherConfiguration;
import com.jnks.iot.server.common.data.device.profile.lwm2m.TelemetryMappingConfiguration;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.security.DeviceCredentials;
import com.jnks.iot.server.common.data.security.DeviceCredentialsType;
import com.jnks.iot.server.common.data.sync.ie.importing.csv.BulkImportColumnType;
import com.jnks.iot.server.common.data.transport.snmp.SnmpProtocolVersion;
import com.jnks.iot.server.dao.device.DeviceCredentialsService;
import com.jnks.iot.server.dao.device.DeviceProfileService;
import com.jnks.iot.server.dao.device.DeviceService;
import com.jnks.iot.server.dao.exception.DeviceCredentialsValidationException;
import com.jnks.iot.server.service.entitiy.device.JnksIotDeviceService;
import com.jnks.iot.server.service.security.model.SecurityUser;
import com.jnks.iot.server.service.sync.ie.importing.csv.AbstractBulkImportService;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.eclipse.leshan.core.LwM2m.Version.V1_0;

/**
 * CSV 设备批量导入实现。
 * <p>
 * 由导入相关 Controller 调用，按列填充设备字段与传输配置（SNMP / MQTT / X.509 / LwM2M 等），
 * 组装凭据后委托 {@link JnksIotDeviceService#saveDeviceWithCredentials} 落库（含审计日志）。
 * 查找已有设备走 {@link DeviceService} DAO；LwM2M 导入会按需创建或改写设备配置。
 *
 * @see AbstractBulkImportService
 * @see JnksIotDeviceService
 */
@Service
@RequiredArgsConstructor
public class DeviceBulkImportService extends AbstractBulkImportService<Device> {
    protected final DeviceService deviceService;
    protected final JnksIotDeviceService jnksIotDeviceService;
    protected final DeviceCredentialsService deviceCredentialsService;
    protected final DeviceProfileService deviceProfileService;

    private final Lock findOrCreateDeviceProfileLock = new ReentrantLock();

    /** 将 CSV 列映射到设备名称、类型、标签、描述与网关标记，并配置 SNMP 等传输参数。 */
    @Override
    protected void setEntityFields(Device device, Map<BulkImportColumnType, String> fields) {
        ObjectNode additionalInfo = getOrCreateAdditionalInfoObj(device);
        fields.forEach((columnType, value) -> {
            switch (columnType) {
                case NAME:
                    device.setName(value);
                    break;
                case TYPE:
                    device.setType(value);
                    break;
                case LABEL:
                    device.setLabel(value);
                    break;
                case DESCRIPTION:
                    additionalInfo.set("description", new TextNode(value));
                    break;
                case IS_GATEWAY:
                    additionalInfo.set("gateway", BooleanNode.valueOf(Boolean.parseBoolean(value)));
                    break;
            }
            device.setAdditionalInfo(additionalInfo);
        });
        setUpDeviceConfiguration(device, fields);
    }

    /** 解析凭据与设备配置后经 {@link JnksIotDeviceService} 保存（含审计副作用）。 */
    @Override
    @SneakyThrows
    protected Device saveEntity(SecurityUser user, Device device, Map<BulkImportColumnType, String> fields) {
        DeviceCredentials deviceCredentials;
        try {
            deviceCredentials = createDeviceCredentials(device.getTenantId(), device.getId(), fields);
            deviceCredentialsService.formatCredentials(deviceCredentials);
        } catch (Exception e) {
            throw new DeviceCredentialsValidationException("Invalid device credentials: " + e.getMessage());
        }

        DeviceProfile deviceProfile;
        if (deviceCredentials.getCredentialsType() == DeviceCredentialsType.LWM2M_CREDENTIALS) {
            deviceProfile = setUpLwM2mDeviceProfile(device.getTenantId(), device);
        } else if (StringUtils.isNotEmpty(device.getType())) {
            deviceProfile = deviceProfileService.findOrCreateDeviceProfile(device.getTenantId(), device.getType());
        } else {
            deviceProfile = deviceProfileService.findDefaultDeviceProfile(device.getTenantId());
        }
        device.setDeviceProfileId(deviceProfile.getId());

        return jnksIotDeviceService.saveDeviceWithCredentials(device, deviceCredentials, user);
    }

    /** 按租户与名称查找设备，不存在则返回空对象供后续填充。 */
    @Override
    protected Device findOrCreateEntity(TenantId tenantId, String name) {
        return Optional.ofNullable(deviceService.findDeviceByTenantIdAndName(tenantId, name))
                .orElseGet(Device::new);
    }

    /** 将导入用户的租户与客户写到设备上。 */
    @Override
    protected void setOwners(Device entity, SecurityUser user) {
        entity.setTenantId(user.getTenantId());
        entity.setCustomerId(user.getCustomerId());
    }

    private void setUpDeviceConfiguration(Device device, Map<BulkImportColumnType, String> fields) {
        if (fields.containsKey(BulkImportColumnType.SNMP_HOST)) {
            SnmpDeviceTransportConfiguration transportConfiguration = new SnmpDeviceTransportConfiguration();
            transportConfiguration.setHost(fields.get(BulkImportColumnType.SNMP_HOST));
            transportConfiguration.setPort(Optional.ofNullable(fields.get(BulkImportColumnType.SNMP_PORT))
                    .map(Integer::parseInt).orElse(161));
            transportConfiguration.setProtocolVersion(Optional.ofNullable(fields.get(BulkImportColumnType.SNMP_VERSION))
                    .map(version -> SnmpProtocolVersion.valueOf(version.toUpperCase())).orElse(SnmpProtocolVersion.V2C));
            transportConfiguration.setCommunity(fields.getOrDefault(BulkImportColumnType.SNMP_COMMUNITY_STRING, "public"));

            DeviceData deviceData = new DeviceData();
            deviceData.setTransportConfiguration(transportConfiguration);
            device.setDeviceData(deviceData);
        }
    }

    @SneakyThrows
    private DeviceCredentials createDeviceCredentials(TenantId tenantId, DeviceId deviceId, Map<BulkImportColumnType, String> fields) {
        DeviceCredentials credentials = new DeviceCredentials();
        if (fields.containsKey(BulkImportColumnType.LWM2M_CLIENT_ENDPOINT)) {
            credentials.setCredentialsType(DeviceCredentialsType.LWM2M_CREDENTIALS);
            setUpLwm2mCredentials(fields, credentials);
        } else if (fields.containsKey(BulkImportColumnType.X509)) {
            credentials.setCredentialsType(DeviceCredentialsType.X509_CERTIFICATE);
            setUpX509CertificateCredentials(fields, credentials);
        } else if (CollectionUtils.containsAny(fields.keySet(), EnumSet.of(BulkImportColumnType.MQTT_CLIENT_ID, BulkImportColumnType.MQTT_USER_NAME, BulkImportColumnType.MQTT_PASSWORD))) {
            credentials.setCredentialsType(DeviceCredentialsType.MQTT_BASIC);
            setUpBasicMqttCredentials(fields, credentials);
        } else if (deviceId != null && !fields.containsKey(BulkImportColumnType.ACCESS_TOKEN)) {
            credentials = deviceCredentialsService.findDeviceCredentialsByDeviceId(tenantId, deviceId);
        } else  {
            credentials.setCredentialsType(DeviceCredentialsType.ACCESS_TOKEN);
            setUpAccessTokenCredentials(fields, credentials);
        }
        return credentials;
    }

    private void setUpAccessTokenCredentials(Map<BulkImportColumnType, String> fields, DeviceCredentials credentials) {
        credentials.setCredentialsId(Optional.ofNullable(fields.get(BulkImportColumnType.ACCESS_TOKEN))
                .orElseGet(() -> StringUtils.randomAlphanumeric(20)));
    }

    private void setUpBasicMqttCredentials(Map<BulkImportColumnType, String> fields, DeviceCredentials credentials) {
        BasicMqttCredentials basicMqttCredentials = new BasicMqttCredentials();
        basicMqttCredentials.setClientId(fields.get(BulkImportColumnType.MQTT_CLIENT_ID));
        basicMqttCredentials.setUserName(fields.get(BulkImportColumnType.MQTT_USER_NAME));
        basicMqttCredentials.setPassword(fields.get(BulkImportColumnType.MQTT_PASSWORD));
        credentials.setCredentialsValue(JacksonUtil.toString(basicMqttCredentials));
    }

    private void setUpX509CertificateCredentials(Map<BulkImportColumnType, String> fields, DeviceCredentials credentials) {
        credentials.setCredentialsValue(fields.get(BulkImportColumnType.X509));
    }

    private void setUpLwm2mCredentials(Map<BulkImportColumnType, String> fields, DeviceCredentials credentials) throws com.fasterxml.jackson.core.JsonProcessingException {
        ObjectNode lwm2mCredentials = JacksonUtil.newObjectNode();

        Set.of(BulkImportColumnType.LWM2M_CLIENT_SECURITY_CONFIG_MODE, BulkImportColumnType.LWM2M_BOOTSTRAP_SERVER_SECURITY_MODE,
                        BulkImportColumnType.LWM2M_SERVER_SECURITY_MODE).stream()
                .map(fields::get)
                .filter(Objects::nonNull)
                .forEach(securityMode -> {
                    try {
                        LwM2MSecurityMode.valueOf(securityMode.toUpperCase());
                    } catch (IllegalArgumentException e) {
                        throw new DeviceCredentialsValidationException("Unknown LwM2M security mode: " + securityMode + ", (the mode should be: NO_SEC, PSK, RPK, X509)!");
                    }
                });

        ObjectNode client = JacksonUtil.newObjectNode();
        setValues(client, fields, Set.of(BulkImportColumnType.LWM2M_CLIENT_SECURITY_CONFIG_MODE,
                BulkImportColumnType.LWM2M_CLIENT_ENDPOINT, BulkImportColumnType.LWM2M_CLIENT_IDENTITY,
                BulkImportColumnType.LWM2M_CLIENT_KEY, BulkImportColumnType.LWM2M_CLIENT_CERT));
        LwM2MClientCredential lwM2MClientCredential = JacksonUtil.treeToValue(client, LwM2MClientCredential.class);
        // so that only fields needed for specific type of lwM2MClientCredentials were saved in json
        lwm2mCredentials.set("client", JacksonUtil.valueToTree(lwM2MClientCredential));

        ObjectNode bootstrapServer = JacksonUtil.newObjectNode();
        setValues(bootstrapServer, fields, Set.of(BulkImportColumnType.LWM2M_BOOTSTRAP_SERVER_SECURITY_MODE,
                BulkImportColumnType.LWM2M_BOOTSTRAP_SERVER_PUBLIC_KEY_OR_ID, BulkImportColumnType.LWM2M_BOOTSTRAP_SERVER_SECRET_KEY));

        ObjectNode lwm2mServer = JacksonUtil.newObjectNode();
        setValues(lwm2mServer, fields, Set.of(BulkImportColumnType.LWM2M_SERVER_SECURITY_MODE,
                BulkImportColumnType.LWM2M_SERVER_CLIENT_PUBLIC_KEY_OR_ID, BulkImportColumnType.LWM2M_SERVER_CLIENT_SECRET_KEY));

        ObjectNode bootstrap = JacksonUtil.newObjectNode();
        bootstrap.set("bootstrapServer", bootstrapServer);
        bootstrap.set("lwm2mServer", lwm2mServer);
        lwm2mCredentials.set("bootstrap", bootstrap);

        credentials.setCredentialsValue(lwm2mCredentials.toString());
    }

    private DeviceProfile setUpLwM2mDeviceProfile(TenantId tenantId, Device device) {
        DeviceProfile deviceProfile = deviceProfileService.findDeviceProfileByName(tenantId, device.getType());
        if (deviceProfile != null) {
            if (deviceProfile.getTransportType() != DeviceTransportType.LWM2M) {
                deviceProfile.setTransportType(DeviceTransportType.LWM2M);
                deviceProfile.getProfileData().setTransportConfiguration(new Lwm2mDeviceProfileTransportConfiguration());
                deviceProfile = deviceProfileService.saveDeviceProfile(deviceProfile);
            }
        } else {
            findOrCreateDeviceProfileLock.lock();
            try {
                deviceProfile = deviceProfileService.findDeviceProfileByName(tenantId, device.getType());
                if (deviceProfile == null) {
                    deviceProfile = new DeviceProfile();
                    deviceProfile.setTenantId(tenantId);
                    deviceProfile.setType(DeviceProfileType.DEFAULT);
                    deviceProfile.setName(device.getType());
                    deviceProfile.setTransportType(DeviceTransportType.LWM2M);
                    deviceProfile.setProvisionType(DeviceProfileProvisionType.DISABLED);

                    Lwm2mDeviceProfileTransportConfiguration transportConfiguration = new Lwm2mDeviceProfileTransportConfiguration();
                    transportConfiguration.setBootstrap(Collections.emptyList());
                    transportConfiguration.setClientLwM2mSettings(new OtherConfiguration(1, 1, 1, PowerMode.DRX, null, null, null, null, null, V1_0.toString()));
                    transportConfiguration.setObserveAttr(new TelemetryMappingConfiguration(Collections.emptyMap(), Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), Collections.emptyMap()));

                    DeviceProfileData deviceProfileData = new DeviceProfileData();
                    DefaultDeviceProfileConfiguration configuration = new DefaultDeviceProfileConfiguration();
                    DisabledDeviceProfileProvisionConfiguration provisionConfiguration = new DisabledDeviceProfileProvisionConfiguration(null);
                    deviceProfileData.setConfiguration(configuration);
                    deviceProfileData.setTransportConfiguration(transportConfiguration);
                    deviceProfileData.setProvisionConfiguration(provisionConfiguration);
                    deviceProfile.setProfileData(deviceProfileData);

                    deviceProfile = deviceProfileService.saveDeviceProfile(deviceProfile);
                }
            } finally {
                findOrCreateDeviceProfileLock.unlock();
            }
        }
        return deviceProfile;
    }

    private void setValues(ObjectNode objectNode, Map<BulkImportColumnType, String> data, Collection<BulkImportColumnType> columns) {
        for (BulkImportColumnType column : columns) {
            String value = StringUtils.defaultString(data.get(column), column.getDefaultValue());
            if (value != null && column.getKey() != null) {
                objectNode.set(column.getKey(), new TextNode(value));
            }
        }
    }

    /** 声明本导入器处理的实体类型为设备。 */
    @Override
    protected EntityType getEntityType() {
        return EntityType.DEVICE;
    }

}
