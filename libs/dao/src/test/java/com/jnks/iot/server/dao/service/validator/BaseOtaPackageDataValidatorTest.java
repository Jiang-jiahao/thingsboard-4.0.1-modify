package com.jnks.iot.server.dao.service.validator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.jnks.iot.server.common.data.OtaPackageInfo;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.ota.OtaPackageType;
import com.jnks.iot.server.dao.device.DeviceProfileDao;
import com.jnks.iot.server.dao.tenant.TenantService;

import java.util.UUID;

import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class BaseOtaPackageDataValidatorTest {


    DeviceProfileDao deviceProfileDao = mock(DeviceProfileDao.class);
    TenantService tenantService = mock(TenantService.class);
    BaseOtaPackageDataValidator<?> validator = spy(BaseOtaPackageDataValidator.class);
    TenantId tenantId = TenantId.fromUUID(UUID.fromString("9ef79cdf-37a8-4119-b682-2e7ed4e018da"));

    @BeforeEach
    void setUp() {
        willReturn(true).given(tenantService).tenantExists(tenantId);
        willReturn(tenantService).given(validator).getTenantService();
        willReturn(deviceProfileDao).given(validator).getDeviceProfileDao();
    }

    @Test
    void testValidateNameInvocation() {
        OtaPackageInfo otaPackageInfo = new OtaPackageInfo();
        otaPackageInfo.setTitle("fw");
        otaPackageInfo.setVersion("1.0");
        otaPackageInfo.setType(OtaPackageType.FIRMWARE);
        otaPackageInfo.setTenantId(tenantId);

        validator.validateImpl(otaPackageInfo);
        verify(validator).validateString("OtaPackage title", otaPackageInfo.getTitle());
    }


}