package com.jnks.iot.server.service.sync.ie.importing.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;
import com.jnks.iot.server.dao.customer.CustomerDao;
import com.jnks.iot.server.dao.customer.CustomerService;
import com.jnks.iot.server.service.sync.vc.data.EntitiesImportCtx;

/**
 * 针对 {@link Customer} 的导入服务，继承 {@link BaseEntityImportService}。
 * <p>
 * 公开客户走 {@code findOrCreatePublicCustomer}，仅回写 externalId 后用 DAO 保存；普通客户走 {@code saveCustomer}。
 * 导出无专用服务，回落 {@link com.jnks.iot.server.service.sync.ie.exporting.impl.DefaultEntityExportService}。
 */
@Service
@RequiredArgsConstructor
public class CustomerImportService extends BaseEntityImportService<CustomerId, Customer, EntityExportData<Customer>> {

    private final CustomerService customerService;
    private final CustomerDao customerDao;

    @Override
    protected void setOwner(TenantId tenantId, Customer customer, IdProvider idProvider) {
        customer.setTenantId(tenantId);
    }

    /**
     * 公开客户复用租户已有 public customer，并带上导出数据中的 externalId。
     */
    @Override
    protected Customer prepare(EntitiesImportCtx ctx, Customer customer, Customer old, EntityExportData<Customer> exportData, IdProvider idProvider) {
        if (customer.isPublic()) {
            Customer publicCustomer = customerService.findOrCreatePublicCustomer(ctx.getTenantId());
            publicCustomer.setExternalId(customer.getExternalId());
            return publicCustomer;
        } else {
            return customer;
        }
    }

    /**
     * 普通客户走业务服务；公开客户直接 DAO 保存以免走创建逻辑。
     */
    @Override
    protected Customer saveOrUpdate(EntitiesImportCtx ctx, Customer customer, EntityExportData<Customer> exportData, IdProvider idProvider) {
        if (!customer.isPublic()) {
            return customerService.saveCustomer(customer);
        } else {
            return customerDao.save(ctx.getTenantId(), customer);
        }
    }

    @Override
    protected Customer deepCopy(Customer customer) {
        return new Customer(customer);
    }

    @Override
    public EntityType getEntityType() {
        return EntityType.CUSTOMER;
    }

}
