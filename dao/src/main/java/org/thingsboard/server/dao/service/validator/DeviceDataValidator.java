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
import org.thingsboard.server.common.data.device.data.DeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.TcpDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.TcpWireAuthenticationMode;
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
        Optional.ofNullable(device.getDeviceData())
                .flatMap(deviceData -> Optional.ofNullable(deviceData.getTransportConfiguration()))
                .ifPresent(DeviceTransportConfiguration::validate);
        validateTcpSharedServerBindPort(tenantId, device);
        // 验证设备（或设备档案）与OTA包的关联关系是否合法。
        validateOtaPackage(tenantId, device, device.getDeviceProfileId());
    }


    /**
     * 同一 {@code serverBindPort} 可对应多台设备，但必须共用同一设备配置文件；
     * 若多台且链路上鉴权为 NONE，则每台须配置互异的 {@code sourceHost}。
     */
    private void validateTcpSharedServerBindPort(TenantId tenantId, Device device) {
        if (device.getDeviceData() == null || !(device.getDeviceData().getTransportConfiguration() instanceof TcpDeviceTransportConfiguration)) {
            return;
        }
        TcpDeviceTransportConfiguration tcp = (TcpDeviceTransportConfiguration) device.getDeviceData().getTransportConfiguration();
        if (tcp.getServerBindPort() == null) {
            return;
        }
        int bindPort = tcp.getServerBindPort();
        UUID currentProfileId = device.getDeviceProfileId().getId();
        UUID currentDeviceUuid = device.getId() != null ? device.getId().getId() : null;
        List<Device> samePort = new ArrayList<>();
        PageLink pageLink = new PageLink(500);
        PageData<Device> page;
        do {
            page = deviceDao.findDevicesByTenantId(tenantId.getId(), pageLink);
            for (Device other : page.getData()) {
                if (currentDeviceUuid != null && other.getId().getId().equals(currentDeviceUuid)) {
                    continue;
                }
                if (other.getDeviceData() == null || !(other.getDeviceData().getTransportConfiguration() instanceof TcpDeviceTransportConfiguration)) {
                    continue;
                }
                TcpDeviceTransportConfiguration ot = (TcpDeviceTransportConfiguration) other.getDeviceData().getTransportConfiguration();
                if (ot.getServerBindPort() == null || ot.getServerBindPort() != bindPort) {
                    continue;
                }
                if (!other.getDeviceProfileId().getId().equals(currentProfileId)) {
                    throw new DataValidationException("TCP serverBindPort " + bindPort
                            + " is already used by a device with a different device profile. "
                            + "The same listen port may only be shared by devices that use the same device profile.");
                }
                samePort.add(other);
            }
            if (!page.hasNext()) {
                break;
            }
            pageLink = pageLink.nextPageLink();
        } while (true);
        samePort.add(device);
        if (samePort.size() <= 1) {
            return;
        }
        DeviceProfile profile = deviceProfileService.findDeviceProfileById(tenantId, device.getDeviceProfileId(), false);
        if (profile == null || profile.getProfileData() == null
                || !(profile.getProfileData().getTransportConfiguration() instanceof TcpDeviceProfileTransportConfiguration)) {
            return;
        }
        TcpDeviceProfileTransportConfiguration ptc = (TcpDeviceProfileTransportConfiguration) profile.getProfileData().getTransportConfiguration();
        if (ptc.getTcpWireAuthenticationMode() != TcpWireAuthenticationMode.NONE) {
            return;
        }
        Set<String> seenHosts = new HashSet<>();
        for (Device d : samePort) {
            TcpDeviceTransportConfiguration dt = (TcpDeviceTransportConfiguration) d.getDeviceData().getTransportConfiguration();
            if (StringUtils.isNotBlank(dt.getSourceHost())) {
                try {
                    String normalized = InetAddress.getByName(dt.getSourceHost().trim()).getHostAddress();
                    if (!seenHosts.add(normalized)) {
                        throw new DataValidationException("Duplicate sourceHost for devices sharing TCP serverBindPort " + bindPort + ".");
                    }
                } catch (UnknownHostException e) {
                    throw new DataValidationException("Invalid sourceHost for shared TCP port: " + dt.getSourceHost());
                }
            } else {
                throw new DataValidationException("When multiple devices share TCP serverBindPort " + bindPort
                        + " with wire authentication NONE, each device must set a distinct sourceHost.");
            }
        }
    }
}
