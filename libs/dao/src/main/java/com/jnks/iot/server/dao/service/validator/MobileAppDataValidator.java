package com.jnks.iot.server.dao.service.validator;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.mobile.app.MobileApp;
import com.jnks.iot.server.common.data.mobile.app.MobileAppStatus;
import com.jnks.iot.server.common.data.oauth2.PlatformType;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.service.DataValidator;

@Component
@AllArgsConstructor
public class MobileAppDataValidator extends DataValidator<MobileApp> {

    @Override
    protected void validateDataImpl(TenantId tenantId, MobileApp mobileApp) {
        if (mobileApp.getStatus() == MobileAppStatus.PUBLISHED) {
            if (mobileApp.getStoreInfo() == null) {
                throw new DataValidationException("Store info is required for published apps");
            }
            if (mobileApp.getPlatformType() == PlatformType.ANDROID &&
                    (mobileApp.getStoreInfo().getSha256CertFingerprints() == null || mobileApp.getStoreInfo().getStoreLink() == null)) {
                throw new DataValidationException("Sha256CertFingerprints and store link are required");
            } else if (mobileApp.getPlatformType() == PlatformType.IOS &&
                    (mobileApp.getStoreInfo().getAppId() == null || mobileApp.getStoreInfo().getStoreLink() == null)) {
                throw new DataValidationException("AppId and store link are required");
            }
        }
    }
}
