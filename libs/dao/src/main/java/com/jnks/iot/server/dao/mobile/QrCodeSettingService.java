package com.jnks.iot.server.dao.mobile;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.mobile.app.MobileApp;
import com.jnks.iot.server.common.data.mobile.qrCodeSettings.QrCodeSettings;
import com.jnks.iot.server.common.data.oauth2.PlatformType;

public interface QrCodeSettingService {

    QrCodeSettings saveQrCodeSettings(TenantId tenantId, QrCodeSettings qrCodeSettings);

    QrCodeSettings findQrCodeSettings(TenantId tenantId);

    MobileApp findAppFromQrCodeSettings(TenantId sysTenantId, PlatformType platformType);

    void deleteByTenantId(TenantId tenantId);

}
