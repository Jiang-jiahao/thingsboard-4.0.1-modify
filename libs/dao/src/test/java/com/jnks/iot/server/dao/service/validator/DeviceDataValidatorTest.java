package com.jnks.iot.server.dao.service.validator;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.device.data.DeviceData;
import com.jnks.iot.server.common.data.device.data.TcpDeviceTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileData;
import com.jnks.iot.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.TcpTransportConnectMode;
import com.jnks.iot.server.common.data.device.profile.TcpWireAuthenticationMode;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.dao.customer.CustomerDao;
import com.jnks.iot.server.dao.device.DeviceDao;
import com.jnks.iot.server.dao.device.DeviceProfileService;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.tenant.TenantService;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.BDDMockito.willReturn;

@SpringBootTest(classes = DeviceDataValidator.class)
@Slf4j
class DeviceDataValidatorTest {

    @MockBean
    DeviceDao deviceDao;
    @MockBean
    TenantService tenantService;
    @MockBean
    CustomerDao customerDao;
    @MockBean
    DeviceProfileService deviceProfileService;
    @Autowired
    DeviceDataValidator validator;
    TenantId tenantId = TenantId.fromUUID(UUID.fromString("9ef79cdf-37a8-4119-b682-2e7ed4e018da"));
    UUID profileUuid = UUID.fromString("2c29a2b5-0000-0000-0000-000000000001");

    @BeforeEach
    void setUp() {
        willReturn(true).given(tenantService).tenantExists(tenantId);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "coffee", "1", "big box", "世界", "!", "--", "~!@#$%^&*()_+=-/|\\[]{};:'`\"?<>,.", "\uD83D\uDC0C", "\041",
            "Gdy Pomorze nie pomoże, to pomoże może morze, a gdy morze nie pomoże, to pomoże może Gdańsk",
    })
    void testDeviceName_thenOK(final String name) {
        Device device = new Device();
        device.setTenantId(tenantId);
        device.setName(name);
        validator.validateDataImpl(tenantId, device);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "", " ", "  ", "\n", "\r\n", "\t", "\000", "\000\000", "\001", "\002", "\040", "\u0000", "\u0000\u0000",
            "F0929906\000\000\000\000\000\000\000\000\000", "\000\000\000F0929906",
            "\u0000F0929906", "F092\u00009906", "F0929906\u0000"
    })
    void testDeviceName_thenDataValidationException(final String name) {
        Device device = new Device();
        device.setTenantId(tenantId);
        device.setName(name);

        DataValidationException exception = Assertions.assertThrows(DataValidationException.class, () -> validator.validateDataImpl(tenantId, device));
        log.warn("Exception message: {}", exception.getMessage());
        assertThat(exception.getMessage()).as("message Device name").containsPattern("Device name .*");
    }

    /** 建档：CLIENT 模式的 NONE 档案不该被当作"共享监听端口"要求 sourceHost。 */
    @Test
    void testTcpClientNoneDevice_thenNoSourceHostRequired() {
        stubTcpProfile(TcpTransportConnectMode.CLIENT);
        // 租户内已存在另一台 NONE 设备（SERVER 语义下正是会触发唯一性校验的场景）
        stubOtherNoneDevice();
        Device device = tcpDevice();          // 默认 host/port，无 sourceHost
        validator.validateDataImpl(tenantId, device);   // 不应抛
    }

    /** 对照：SERVER 模式下同样场景仍应要求 sourceHost（修复未误伤原行为）。 */
    @Test
    void testTcpServerNoneDevice_thenSourceHostRequired() {
        stubTcpProfile(TcpTransportConnectMode.SERVER);
        stubOtherNoneDevice();
        Device device = tcpDevice();
        Assertions.assertThrows(DataValidationException.class, () -> validator.validateDataImpl(tenantId, device));
    }

    private void stubTcpProfile(TcpTransportConnectMode connectMode) {
        TcpDeviceProfileTransportConfiguration tc = new TcpDeviceProfileTransportConfiguration();
        tc.setTcpTransportConnectMode(connectMode);
        tc.setTcpWireAuthenticationMode(TcpWireAuthenticationMode.NONE);
        DeviceProfileData pd = new DeviceProfileData();
        pd.setTransportConfiguration(tc);
        DeviceProfile profile = new DeviceProfile();
        profile.setId(new DeviceProfileId(profileUuid));
        profile.setTenantId(tenantId);
        profile.setName("tcp-profile");
        profile.setProfileData(pd);
        willReturn(profile).given(deviceProfileService).findDeviceProfileById(any(), any(), anyBoolean());
    }

    private void stubOtherNoneDevice() {
        willReturn(new PageData<>(List.of(tcpDevice()), 1, 1, false))
                .given(deviceDao).findDevicesByTenantId(any(), any());
    }

    private Device tcpDevice() {
        Device device = new Device();
        device.setId(new DeviceId(UUID.randomUUID()));
        device.setTenantId(tenantId);
        device.setName("tcp-dev-" + UUID.randomUUID());
        device.setDeviceProfileId(new DeviceProfileId(profileUuid));
        DeviceData dd = new DeviceData();
        dd.setTransportConfiguration(new TcpDeviceTransportConfiguration());
        device.setDeviceData(dd);
        return device;
    }

}
