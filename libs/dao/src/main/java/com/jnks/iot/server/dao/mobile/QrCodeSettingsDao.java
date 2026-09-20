package com.jnks.iot.server.dao.mobile;

import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.mobile.qrCodeSettings.QrCodeSettings;
import com.jnks.iot.server.dao.Dao;


public interface QrCodeSettingsDao extends Dao<QrCodeSettings> {

    QrCodeSettings findByTenantId(TenantId tenantId);

    void removeByTenantId(TenantId tenantId);
}
