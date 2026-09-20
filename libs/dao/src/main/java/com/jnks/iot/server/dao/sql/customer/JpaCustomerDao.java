package com.jnks.iot.server.dao.sql.customer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.edqs.fields.CustomerFields;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.DaoUtil;
import com.jnks.iot.server.dao.customer.CustomerDao;
import com.jnks.iot.server.dao.model.sql.CustomerEntity;
import com.jnks.iot.server.dao.sql.JpaAbstractDao;
import com.jnks.iot.server.dao.util.SqlDao;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Created by Valerii Sosliuk on 5/6/2017.
 */
@Component
@SqlDao
public class JpaCustomerDao extends JpaAbstractDao<CustomerEntity, Customer> implements CustomerDao {

    @Autowired
    private CustomerRepository customerRepository;

    @Override
    protected Class<CustomerEntity> getEntityClass() {
        return CustomerEntity.class;
    }

    @Override
    protected JpaRepository<CustomerEntity, UUID> getRepository() {
        return customerRepository;
    }

    @Override
    public PageData<Customer> findCustomersByTenantId(UUID tenantId, PageLink pageLink) {
        return DaoUtil.toPageData(customerRepository.findByTenantId(
                tenantId,
                pageLink.getTextSearch(),
                DaoUtil.toPageable(pageLink)));
    }

    @Override
    public Optional<Customer> findCustomerByTenantIdAndTitle(UUID tenantId, String title) {
        return Optional.ofNullable(DaoUtil.getData(customerRepository.findByTenantIdAndTitle(tenantId, title)));
    }

    @Override
    public Optional<Customer> findPublicCustomerByTenantId(UUID tenantId) {
        return Optional.ofNullable(DaoUtil.getData(customerRepository.findPublicCustomerByTenantId(tenantId)));
    }

    @Override
    public Long countByTenantId(TenantId tenantId) {
        return customerRepository.countByTenantId(tenantId.getId());
    }

    @Override
    public Customer findByTenantIdAndExternalId(UUID tenantId, UUID externalId) {
        return DaoUtil.getData(customerRepository.findByTenantIdAndExternalId(tenantId, externalId));
    }

    @Override
    public Customer findByTenantIdAndName(UUID tenantId, String name) {
        return findCustomerByTenantIdAndTitle(tenantId, name).orElse(null);
    }

    @Override
    public PageData<Customer> findByTenantId(UUID tenantId, PageLink pageLink) {
        return findCustomersByTenantId(tenantId, pageLink);
    }

    @Override
    public CustomerId getExternalIdByInternal(CustomerId internalId) {
        return Optional.ofNullable(customerRepository.getExternalIdById(internalId.getId()))
                .map(CustomerId::new).orElse(null);
    }

    @Override
    public PageData<Customer> findCustomersWithTheSameTitle(PageLink pageLink) {
        return DaoUtil.toPageData(
                customerRepository.findCustomersWithTheSameTitle(DaoUtil.toPageable(pageLink))
        );
    }

    @Override
    public PageData<Customer> findAllByTenantId(TenantId tenantId, PageLink pageLink) {
        return findByTenantId(tenantId.getId(), pageLink);
    }

    @Override
    public List<CustomerFields> findNextBatch(UUID id, int batchSize) {
        return customerRepository.findNextBatch(id, Limit.of(batchSize));
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.CUSTOMER;
    }

}
