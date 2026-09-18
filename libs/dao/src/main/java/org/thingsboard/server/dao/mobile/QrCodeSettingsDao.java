package org.thingsboard.server.dao.mobile;

import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.mobile.qrCodeSettings.QrCodeSettings;
import org.thingsboard.server.dao.Dao;


public interface QrCodeSettingsDao extends Dao<QrCodeSettings> {

    QrCodeSettings findByTenantId(TenantId tenantId);

    void removeByTenantId(TenantId tenantId);
}
