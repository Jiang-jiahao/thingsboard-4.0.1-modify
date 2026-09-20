package com.jnks.iot.server.dao.sql.mobile;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.mobile.qrCodeSettings.QrCodeSettings;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.mobile.QrCodeSettingsDao;
import com.jnks.iot.server.dao.model.sql.QrCodeSettingsEntity;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.UUID;


@Component
@Slf4j
@SqlDao
public class JpaQrCodeSettingsDao extends JpaAbstractDao<QrCodeSettingsEntity, QrCodeSettings> implements QrCodeSettingsDao {

    @Autowired
    private QrCodeSettingsRepository qrCodeSettingsRepository;


    @Override
    public QrCodeSettings findByTenantId(TenantId tenantId) {
        return DaoUtil.getData(qrCodeSettingsRepository.findByTenantId(tenantId.getId()));
    }

    @Override
    public void removeByTenantId(TenantId tenantId) {
        qrCodeSettingsRepository.deleteByTenantId(tenantId.getId());
    }

    @Override
    protected Class<QrCodeSettingsEntity> getEntityClass() {
        return QrCodeSettingsEntity.class;
    }

    @Override
    protected JpaRepository<QrCodeSettingsEntity, UUID> getRepository() {
        return qrCodeSettingsRepository;
    }
}
