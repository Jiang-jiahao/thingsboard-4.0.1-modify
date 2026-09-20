package com.jnks.iot.server.dao.model.sql;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.mobile.bundle.MobileAppBundleInfo;

@Data
@EqualsAndHashCode(callSuper = true)
public class MobileAppBundleInfoEntity extends AbstractMobileAppBundleEntity<MobileAppBundleInfo> {

    private String androidPkgName;
    private String iosPkgName;
    private boolean qrCodeEnabled;

    public MobileAppBundleInfoEntity() {
        super();
    }

    public MobileAppBundleInfoEntity(MobileAppBundleEntity mobileAppBundleEntity, String androidPkgName, String iosPkgName, boolean qrCodeEnabled) {
        super(mobileAppBundleEntity);
        this.androidPkgName = androidPkgName;
        this.iosPkgName = iosPkgName;
        this.qrCodeEnabled = qrCodeEnabled;
    }

    @Override
    public MobileAppBundleInfo toData() {
        return new MobileAppBundleInfo(super.toMobileAppBundle(), androidPkgName, iosPkgName, qrCodeEnabled);
    }
}
