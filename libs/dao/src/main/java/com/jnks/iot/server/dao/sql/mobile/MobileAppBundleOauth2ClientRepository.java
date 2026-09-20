package com.jnks.iot.server.dao.sql.mobile;

import org.springframework.data.jpa.repository.JpaRepository;
import com.jnks.iot.server.dao.model.sql.MobileAppOauth2ClientCompositeKey;
import com.jnks.iot.server.dao.model.sql.MobileAppBundleOauth2ClientEntity;

import java.util.List;
import java.util.UUID;

public interface MobileAppBundleOauth2ClientRepository extends JpaRepository<MobileAppBundleOauth2ClientEntity, MobileAppOauth2ClientCompositeKey> {

    List<MobileAppBundleOauth2ClientEntity> findAllByMobileAppBundleId(UUID mobileAppId);

}
