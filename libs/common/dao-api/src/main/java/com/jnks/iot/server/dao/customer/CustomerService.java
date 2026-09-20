package com.jnks.iot.server.dao.customer;

import com.google.common.util.concurrent.ListenableFuture;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.entity.EntityDaoService;

import java.util.Optional;

public interface CustomerService extends EntityDaoService {

    Customer findCustomerById(TenantId tenantId, CustomerId customerId);

    Optional<Customer> findCustomerByTenantIdAndTitle(TenantId tenantId, String title);

    ListenableFuture<Optional<Customer>> findCustomerByTenantIdAndTitleAsync(TenantId tenantId, String title);

    ListenableFuture<Customer> findCustomerByIdAsync(TenantId tenantId, CustomerId customerId);

    Customer saveCustomer(Customer customer);

    void deleteCustomer(TenantId tenantId, CustomerId customerId);

    Customer findOrCreatePublicCustomer(TenantId tenantId);

    Customer findPublicCustomer(TenantId tenantId);

    PageData<Customer> findCustomersByTenantId(TenantId tenantId, PageLink pageLink);

    void deleteCustomersByTenantId(TenantId tenantId);

}
