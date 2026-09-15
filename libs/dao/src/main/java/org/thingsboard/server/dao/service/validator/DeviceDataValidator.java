/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.dao.service.validator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.data.DeviceData;
import org.thingsboard.server.common.data.device.data.DeviceScheduledRpc;
import org.thingsboard.server.common.data.device.data.DeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.TcpDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.UdpDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.UdpDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.TcpTransportConnectMode;
import org.thingsboard.server.common.data.device.profile.TcpWireAuthenticationMode;
import org.thingsboard.server.common.data.device.profile.UdpWireAuthenticationMode;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.dao.customer.CustomerDao;
import org.thingsboard.server.dao.device.DeviceDao;
import org.thingsboard.server.dao.device.DeviceProfileService;
import org.thingsboard.server.dao.exception.DataValidationException;
import org.thingsboard.server.dao.tenant.TenantService;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;


import static org.thingsboard.server.dao.model.ModelConstants.NULL_UUID;

@Component
public class DeviceDataValidator extends AbstractHasOtaPackageValidator<Device> {

    @Autowired
    private DeviceDao deviceDao;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private CustomerDao customerDao;

    @Autowired
    private DeviceProfileService deviceProfileService;

    @Override
    protected void validateCreate(TenantId tenantId, Device device) {
        validateNumberOfEntitiesPerTenant(tenantId, EntityType.DEVICE);
    }

    @Override
    protected Device validateUpdate(TenantId tenantId, Device device) {
        Device old = deviceDao.findById(device.getTenantId(), device.getId().getId());
        if (old == null) {
            throw new DataValidationException("Can't update non existing device!");
        }
        return old;
    }

    @Override
    protected void validateDataImpl(TenantId tenantId, Device device) {
        validateString("Device name", device.getName());
        if (device.getTenantId() == null) {
            throw new DataValidationException("Device should be assigned to tenant!");
        } else {
            if (!tenantService.tenantExists(device.getTenantId())) {
                throw new DataValidationException("Device is referencing to non-existent tenant!");
            }
        }
        if (device.getCustomerId() == null) {
            device.setCustomerId(new CustomerId(NULL_UUID));
        } else if (!device.getCustomerId().getId().equals(NULL_UUID)) {
            Customer customer = customerDao.findById(device.getTenantId(), device.getCustomerId().getId());
            if (customer == null) {
                throw new DataValidationException("Can't assign device to non-existent customer!");
            }
            if (!customer.getTenantId().getId().equals(device.getTenantId().getId())) {
                throw new DataValidationException("Can't assign device to customer from different tenant!");
            }
        }
        ensureTcpDeviceTransportForDeferredPayloadDeviceIdProfile(device);
        Optional.ofNullable(device.getDeviceData())
                .flatMap(deviceData -> Optional.ofNullable(deviceData.getTransportConfiguration()))
                .ifPresent(DeviceTransportConfiguration::validate);
        validateScheduledRpcs(device);
        validateTcpWireAuthPayloadDeviceIdWhenRequired(device);
        validateTcpWireIdentityUniquePerTenant(tenantId, device);
        ensureUdpDeviceTransportForDeferredPayloadDeviceIdProfile(device);
        validateUdpWireAuthPayloadDeviceIdWhenRequired(device);
        validateUdpWireIdentityUniquePerTenant(tenantId, device);
        // 验证设备（或设备档案）与OTA包的关联关系是否合法。
        validateOtaPackage(tenantId, device, device.getDeviceProfileId());
    }

    private void validateScheduledRpcs(Device device) {
        if (device.getDeviceData() == null || device.getDeviceData().getScheduledRpcs() == null) {
            return;
        }
        Set<String> methodIds = new HashSet<>();
        for (DeviceScheduledRpc scheduled : device.getDeviceData().getScheduledRpcs()) {
            if (scheduled == null) {
                continue;
            }
            try {
                scheduled.validate();
            } catch (IllegalArgumentException e) {
                throw new DataValidationException(e.getMessage());
            }
            if (StringUtils.isNotBlank(scheduled.getMethodId()) && !methodIds.add(scheduled.getMethodId())) {
                throw new DataValidationException("Duplicate scheduled RPC methodId: " + scheduled.getMethodId());
            }
        }
    }


    /**
     * 共享监听端口下端口不再起消歧作用：{@link TcpWireAuthenticationMode#NONE} 靠 {@code sourceHost}、
     * {@link TcpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 靠协议设备号识别设备，因此这两种模式的身份串
     * 必须在<strong>租户内</strong>唯一（原先的范围是"同一专用监听端口内"）。与旧行为一致：仅当该模式下租户内
     * 存在多台设备时才强制校验（且不得为空）。
     */
    private void validateTcpWireIdentityUniquePerTenant(TenantId tenantId, Device device) {
        if (device.getDeviceData() == null
                || !(device.getDeviceData().getTransportConfiguration() instanceof TcpDeviceTransportConfiguration current)) {
            return;
        }
        DeviceProfile profile = deviceProfileService.findDeviceProfileById(tenantId, device.getDeviceProfileId(), false);
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration ptc)) {
            return;
        }
        TcpWireAuthenticationMode wireMode = ptc.getTcpWireAuthenticationMode();
        if (wireMode != TcpWireAuthenticationMode.NONE
                && wireMode != TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            return;
        }
        String field = wireMode == TcpWireAuthenticationMode.NONE ? "sourceHost" : "tcpWireAuthPayloadDeviceId";
        String identity = normalizeTcpWireIdentity(wireMode, current);
        UUID selfUuid = device.getId() != null ? device.getId().getId() : null;
        boolean otherExists = false;
        Set<String> others = new HashSet<>();
        PageLink pageLink = new PageLink(500);
        PageData<Device> page;
        do {
            page = deviceDao.findDevicesByTenantId(tenantId.getId(), pageLink);
            for (Device other : page.getData()) {
                if (selfUuid != null && selfUuid.equals(other.getId().getId())) {
                    continue;
                }
                if (other.getDeviceData() == null
                        || !(other.getDeviceData().getTransportConfiguration() instanceof TcpDeviceTransportConfiguration ot)) {
                    continue;
                }
                DeviceProfile otherProfile = deviceProfileService.findDeviceProfileById(tenantId, other.getDeviceProfileId(), false);
                if (otherProfile == null || otherProfile.getProfileData() == null
                        || !(otherProfile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration optc)
                        || optc.getTcpWireAuthenticationMode() != wireMode) {
                    continue;
                }
                otherExists = true;
                String otherIdentity = normalizeTcpWireIdentity(wireMode, ot);
                if (otherIdentity != null) {
                    others.add(otherIdentity);
                }
            }
            if (!page.hasNext()) {
                break;
            }
            pageLink = pageLink.nextPageLink();
        } while (true);
        if (!otherExists) {
            return;
        }
        if (identity == null) {
            throw new DataValidationException("With a shared TCP listen port, each device using " + wireMode
                    + " must set a non-empty " + field + " (it is the only way to identify the device).");
        }
        if (others.contains(identity)) {
            throw new DataValidationException("Duplicate " + field + " '" + identity
                    + "' within the tenant: with a shared TCP listen port the identity must be unique per tenant.");
        }
    }

    private static String normalizeTcpWireIdentity(TcpWireAuthenticationMode wireMode, TcpDeviceTransportConfiguration cfg) {
        if (wireMode == TcpWireAuthenticationMode.NONE) {
            return StringUtils.isBlank(cfg.getSourceHost()) ? null : cfg.getSourceHost().trim();
        }
        if (wireMode == TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            return StringUtils.isBlank(cfg.getTcpWireAuthPayloadDeviceId()) ? null : cfg.getTcpWireAuthPayloadDeviceId().trim();
        }
        return null;
    }

    /**
     * UDP 侧与 TCP 同构，见 {@link #validateTcpWireIdentityUniquePerTenant}。
     */
    private void validateUdpWireIdentityUniquePerTenant(TenantId tenantId, Device device) {
        if (device.getDeviceData() == null
                || !(device.getDeviceData().getTransportConfiguration() instanceof UdpDeviceTransportConfiguration current)) {
            return;
        }
        DeviceProfile profile = deviceProfileService.findDeviceProfileById(tenantId, device.getDeviceProfileId(), false);
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration ptc)) {
            return;
        }
        UdpWireAuthenticationMode wireMode = ptc.getUdpWireAuthenticationMode();
        if (wireMode != UdpWireAuthenticationMode.NONE
                && wireMode != UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            return;
        }
        String field = wireMode == UdpWireAuthenticationMode.NONE ? "sourceHost" : "udpWireAuthPayloadDeviceId";
        String identity = normalizeUdpWireIdentity(wireMode, current);
        UUID selfUuid = device.getId() != null ? device.getId().getId() : null;
        boolean otherExists = false;
        Set<String> others = new HashSet<>();
        PageLink pageLink = new PageLink(500);
        PageData<Device> page;
        do {
            page = deviceDao.findDevicesByTenantId(tenantId.getId(), pageLink);
            for (Device other : page.getData()) {
                if (selfUuid != null && selfUuid.equals(other.getId().getId())) {
                    continue;
                }
                if (other.getDeviceData() == null
                        || !(other.getDeviceData().getTransportConfiguration() instanceof UdpDeviceTransportConfiguration ot)) {
                    continue;
                }
                DeviceProfile otherProfile = deviceProfileService.findDeviceProfileById(tenantId, other.getDeviceProfileId(), false);
                if (otherProfile == null || otherProfile.getProfileData() == null
                        || !(otherProfile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration optc)
                        || optc.getUdpWireAuthenticationMode() != wireMode) {
                    continue;
                }
                otherExists = true;
                String otherIdentity = normalizeUdpWireIdentity(wireMode, ot);
                if (otherIdentity != null) {
                    others.add(otherIdentity);
                }
            }
            if (!page.hasNext()) {
                break;
            }
            pageLink = pageLink.nextPageLink();
        } while (true);
        if (!otherExists) {
            return;
        }
        if (identity == null) {
            throw new DataValidationException("With a shared UDP listen port, each device using " + wireMode
                    + " must set a non-empty " + field + " (it is the only way to identify the device).");
        }
        if (others.contains(identity)) {
            throw new DataValidationException("Duplicate " + field + " '" + identity
                    + "' within the tenant: with a shared UDP listen port the identity must be unique per tenant.");
        }
    }

    private static String normalizeUdpWireIdentity(UdpWireAuthenticationMode wireMode, UdpDeviceTransportConfiguration cfg) {
        if (wireMode == UdpWireAuthenticationMode.NONE) {
            return StringUtils.isBlank(cfg.getSourceHost()) ? null : cfg.getSourceHost().trim();
        }
        if (wireMode == UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            return StringUtils.isBlank(cfg.getUdpWireAuthPayloadDeviceId()) ? null : cfg.getUdpWireAuthPayloadDeviceId().trim();
        }
        return null;
    }

    /**
     * 设备档案为 {@link TcpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 时，部分客户端/表单路径可能未提交
     * {@code deviceData.transportConfiguration}，导致反序列化后无法作为 {@link TcpDeviceTransportConfiguration} 校验。
     * 在此补默认 TCP 传输（与 {@link TcpDeviceTransportConfiguration} 无参构造一致），后续仍强制校验
     * {@link TcpDeviceTransportConfiguration#getTcpWireAuthPayloadDeviceId()} 非空。
     */
    private void ensureTcpDeviceTransportForDeferredPayloadDeviceIdProfile(Device device) {
        if (device.getDeviceProfileId() == null) {
            return;
        }
        DeviceProfile profile = deviceProfileService.findDeviceProfileById(device.getTenantId(), device.getDeviceProfileId(), false);
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration ptc)) {
            return;
        }
        if (ptc.getTcpWireAuthenticationMode() != TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            return;
        }
        if (device.getDeviceData() == null) {
            device.setDeviceData(new DeviceData());
        }
        DeviceTransportConfiguration tc = device.getDeviceData().getTransportConfiguration();
        if (tc == null) {
            device.getDeviceData().setTransportConfiguration(new TcpDeviceTransportConfiguration());
        } else if (!(tc instanceof TcpDeviceTransportConfiguration)) {
            throw new DataValidationException(
                    "TCP DEFERRED_PAYLOAD_DEVICE_ID requires device transport configuration of type TCP (JSON must include \"type\":\"TCP\" under transportConfiguration).");
        }
    }

    /**
     * 档案为 DEFERRED_PAYLOAD_DEVICE_ID 时，设备传输上须配置与负载 JSON 字段比对的协议设备 ID。
     */
    private void validateTcpWireAuthPayloadDeviceIdWhenRequired(Device device) {
        DeviceProfile profile = deviceProfileService.findDeviceProfileById(device.getTenantId(), device.getDeviceProfileId(), false);
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration)) {
            return;
        }
        TcpDeviceProfileTransportConfiguration ptc =
                (TcpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration();
        if (ptc.getTcpWireAuthenticationMode() != TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            return;
        }
        if (device.getDeviceData() == null || !(device.getDeviceData().getTransportConfiguration() instanceof TcpDeviceTransportConfiguration)) {
            throw new DataValidationException("TCP DEFERRED_PAYLOAD_DEVICE_ID requires device transport configuration.");
        }
        TcpDeviceTransportConfiguration tcp = (TcpDeviceTransportConfiguration) device.getDeviceData().getTransportConfiguration();
        if (StringUtils.isBlank(tcp.getTcpWireAuthPayloadDeviceId())) {
            throw new DataValidationException(
                    "TCP DEFERRED_PAYLOAD_DEVICE_ID requires tcpWireAuthPayloadDeviceId on the device transport configuration (must match the payload JSON field value; the value must be unique within the tenant).");
        }
    }

    private void ensureUdpDeviceTransportForDeferredPayloadDeviceIdProfile(Device device) {
        if (device.getDeviceProfileId() == null) {
            return;
        }
        DeviceProfile profile = deviceProfileService.findDeviceProfileById(device.getTenantId(), device.getDeviceProfileId(), false);
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration ptc)) {
            return;
        }
        if (ptc.getUdpWireAuthenticationMode() != UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            return;
        }
        if (device.getDeviceData() == null) {
            device.setDeviceData(new DeviceData());
        }
        DeviceTransportConfiguration tc = device.getDeviceData().getTransportConfiguration();
        if (tc == null) {
            device.getDeviceData().setTransportConfiguration(new UdpDeviceTransportConfiguration());
        } else if (!(tc instanceof UdpDeviceTransportConfiguration)) {
            throw new DataValidationException(
                    "UDP DEFERRED_PAYLOAD_DEVICE_ID requires device transport configuration of type UDP (JSON must include \"type\":\"UDP\" under transportConfiguration).");
        }
    }

    private void validateUdpWireAuthPayloadDeviceIdWhenRequired(Device device) {
        DeviceProfile profile = deviceProfileService.findDeviceProfileById(device.getTenantId(), device.getDeviceProfileId(), false);
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof UdpDeviceProfileTransportConfiguration)) {
            return;
        }
        UdpDeviceProfileTransportConfiguration ptc =
                (UdpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration();
        if (ptc.getUdpWireAuthenticationMode() != UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
            return;
        }
        if (device.getDeviceData() == null || !(device.getDeviceData().getTransportConfiguration() instanceof UdpDeviceTransportConfiguration)) {
            throw new DataValidationException("UDP DEFERRED_PAYLOAD_DEVICE_ID requires device transport configuration.");
        }
        UdpDeviceTransportConfiguration udp = (UdpDeviceTransportConfiguration) device.getDeviceData().getTransportConfiguration();
        if (StringUtils.isBlank(udp.getUdpWireAuthPayloadDeviceId())) {
            throw new DataValidationException(
                    "UDP DEFERRED_PAYLOAD_DEVICE_ID requires udpWireAuthPayloadDeviceId on the device transport configuration (must match the payload JSON field value; the value must be unique within the tenant).");
        }
    }

}
