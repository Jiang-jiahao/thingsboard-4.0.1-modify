package com.jnks.iot.server.dao.sql.customer;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import com.jnks.iot.server.common.data.Customer;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.dao.AbstractJpaDaoTest;
import com.jnks.iot.server.dao.customer.CustomerDao;
import com.jnks.iot.server.dao.customer.CustomerServiceImpl;

import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Valerii Sosliuk on 5/6/2017.
 */
public class JpaCustomerDaoTest extends AbstractJpaDaoTest {

    @Autowired
    private CustomerDao customerDao;

    @Test
    public void testFindByTenantId() {
        UUID tenantId1 = Uuids.timeBased();
        UUID tenantId2 = Uuids.timeBased();

        for (int i = 0; i < 20; i++) {
            createCustomer(tenantId1, i);
            createCustomer(tenantId2, i * 2);
        }

        PageLink pageLink = new PageLink(15, 0,  "CUSTOMER");
        PageData<Customer> customers1 = customerDao.findCustomersByTenantId(tenantId1, pageLink);
        assertEquals(15, customers1.getData().size());

        pageLink = pageLink.nextPageLink();
        PageData<Customer> customers2 = customerDao.findCustomersByTenantId(tenantId1, pageLink);
        assertEquals(5, customers2.getData().size());
    }

    @Test
    public void testFindCustomersByTenantIdAndTitle() {
        UUID tenantId = Uuids.timeBased();

        for (int i = 0; i < 10; i++) {
            createCustomer(tenantId, i);
        }

        Optional<Customer> customerOpt = customerDao.findCustomerByTenantIdAndTitle(tenantId, "CUSTOMER_5");
        assertTrue(customerOpt.isPresent());
        assertEquals("CUSTOMER_5", customerOpt.get().getTitle());
    }

    @Test
    public void testFindPublicCustomerByTenantId() {
        UUID tenantId = Uuids.timeBased();

        Optional<Customer> customerOpt = customerDao.findPublicCustomerByTenantId(tenantId);
        assertTrue(customerOpt.isEmpty());

        String publicCustomerTitle = StringUtils.randomAlphanumeric(10);
        createPublicCustomer(tenantId, publicCustomerTitle);
        customerOpt = customerDao.findPublicCustomerByTenantId(tenantId);
        assertTrue(customerOpt.isPresent());
        Customer customer = customerOpt.get();
        assertTrue(customer.isPublic());
        assertEquals(publicCustomerTitle, customer.getTitle());
    }

    private void createCustomer(UUID tenantId, int index) {
        Customer customer = new Customer();
        customer.setId(new CustomerId(Uuids.timeBased()));
        customer.setTenantId(TenantId.fromUUID(tenantId));
        customer.setTitle("CUSTOMER_" + index);
        customerDao.save(TenantId.fromUUID(tenantId), customer);
    }

    private void createPublicCustomer(UUID tenantId, String publicCustomerTitle) {
        Customer customer = new Customer();
        customer.setId(new CustomerId(Uuids.timeBased()));
        customer.setTenantId(TenantId.fromUUID(tenantId));
        customer.setTitle(publicCustomerTitle);
        customer.setAdditionalInfo(CustomerServiceImpl.PUBLIC_CUSTOMER_ADDITIONAL_INFO_JSON);
        customerDao.save(TenantId.fromUUID(tenantId), customer);
    }
}
