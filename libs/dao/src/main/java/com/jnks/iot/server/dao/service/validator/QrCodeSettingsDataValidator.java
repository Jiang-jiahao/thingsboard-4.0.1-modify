package com.jnks.iot.server.dao.service.validator;

import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.id.MobileAppBundleId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.mobile.app.MobileApp;
import com.jnks.iot.server.common.data.mobile.app.MobileAppStatus;
import com.jnks.iot.server.common.data.mobile.qrCodeSettings.QrCodeSettings;
import com.jnks.iot.server.common.data.oauth2.PlatformType;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.mobile.MobileAppDao;
import com.jnks.iot.server.dao.service.DataValidator;

@Component
@AllArgsConstructor
public class QrCodeSettingsDataValidator extends DataValidator<QrCodeSettings> {

    @Autowired
    MobileAppDao mobileAppDao;

    @Override
    protected void validateDataImpl(TenantId tenantId, QrCodeSettings qrCodeSettings) {
        MobileAppBundleId mobileAppBundleId = qrCodeSettings.getMobileAppBundleId();
        if (!qrCodeSettings.isUseDefaultApp() && (mobileAppBundleId == null)) {
            throw new DataValidationException("Mobile app bundle is required to use custom application!");
        }
        if (!qrCodeSettings.isUseDefaultApp()) {
            if (qrCodeSettings.isAndroidEnabled()) {
                MobileApp androidApp = mobileAppDao.findByBundleIdAndPlatformType(tenantId, mobileAppBundleId, PlatformType.ANDROID);
                if (androidApp != null && androidApp.getStatus() != MobileAppStatus.PUBLISHED) {
                    throw new DataValidationException("The mobile app bundle references an Android app that has not been published!");
                }
            }
            if (qrCodeSettings.isIosEnabled()) {
                MobileApp iosApp = mobileAppDao.findByBundleIdAndPlatformType(tenantId, mobileAppBundleId, PlatformType.IOS);
                if (iosApp != null && iosApp.getStatus() != MobileAppStatus.PUBLISHED) {
                    throw new DataValidationException("The mobile app bundle references an iOS app that has not been published!");
                }
            }
        }
    }
}
