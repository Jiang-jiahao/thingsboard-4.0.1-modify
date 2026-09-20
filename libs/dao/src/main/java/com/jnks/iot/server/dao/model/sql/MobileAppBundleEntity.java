package com.jnks.iot.server.dao.model.sql;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.mobile.bundle.MobileAppBundle;

import static com.jnks.iot.server.dao.model.ModelConstants.MOBILE_APP_BUNDLE_TABLE_NAME;

@Data
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name = MOBILE_APP_BUNDLE_TABLE_NAME)
public final class MobileAppBundleEntity extends AbstractMobileAppBundleEntity<MobileAppBundle> {

    public MobileAppBundleEntity() {
        super();
    }

    public MobileAppBundleEntity(MobileAppBundle mobileAppBundle) {
        super(mobileAppBundle);
    }

    @Override
    public MobileAppBundle toData() {
        return super.toMobileAppBundle();
    }
}
