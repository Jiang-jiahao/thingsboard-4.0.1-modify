package com.jnks.iot.server.dao.service;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.springframework.beans.factory.annotation.Autowired;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.Tenant;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.page.PageLink;
import com.jnks.iot.server.common.data.queue.QueueStats;
import com.jnks.iot.server.dao.exception.DataValidationException;
import com.jnks.iot.server.dao.queue.QueueStatsService;

import static org.assertj.core.api.Assertions.assertThat;


@DaoSqlTest
public class QueueStatsServiceTest extends AbstractServiceTest {

    @Autowired
    QueueStatsService queueStatsService;

    private TenantId tenantId;

    @Before
    public void before() throws NoSuchFieldException, IllegalAccessException {

        Tenant tenant = new Tenant();
        tenant.setTitle("My tenant");
        Tenant savedTenant = tenantService.saveTenant(tenant);
        Assert.assertNotNull(savedTenant);
        tenantId = savedTenant.getId();
    }

    @After
    public void after() {
        tenantService.deleteTenant(tenantId);
    }

    @Test
    public void testSaveQueueStats() {
        QueueStats queueStats = new QueueStats();
        queueStats.setTenantId(tenantId);
        String queueName = StringUtils.randomAlphabetic(8);
        queueStats.setQueueName(queueName);
        queueStats.setServiceId(StringUtils.randomAlphabetic(8));

        QueueStats savedQueueStats = queueStatsService.save(tenantId, queueStats);
        Assert.assertNotNull(savedQueueStats);
        Assert.assertNotNull(savedQueueStats.getId());
        Assert.assertTrue(savedQueueStats.getCreatedTime() > 0);
        Assert.assertEquals(queueStats.getTenantId(), savedQueueStats.getTenantId());
        Assert.assertEquals(savedQueueStats.getQueueName(), queueStats.getQueueName());

        QueueStats retrievedQueueStatsById = queueStatsService.findQueueStatsById(tenantId, savedQueueStats.getId());
        Assert.assertEquals(retrievedQueueStatsById.getQueueName(), queueName);

        String secondQueueName = StringUtils.randomAlphabetic(8);
        queueStats.setQueueName(secondQueueName);
        QueueStats savedQueueStats2 = queueStatsService.save(tenantId, queueStats);
        QueueStats retrievedQueueStatsById2 = queueStatsService.findQueueStatsById(tenantId, savedQueueStats2.getId());
        Assert.assertEquals(retrievedQueueStatsById2.getQueueName(), secondQueueName);

        PageData<QueueStats> queueStatsList = queueStatsService.findByTenantId(tenantId, new PageLink(10));
        Assert.assertEquals(2, queueStatsList.getData().size());
        assertThat(queueStatsList.getData()).containsOnly(retrievedQueueStatsById, retrievedQueueStatsById2);

        queueStatsService.deleteByTenantId(tenantId);
        QueueStats retrievedQueueStatsAfterDelete = queueStatsService.findQueueStatsById(tenantId, savedQueueStats.getId());
        Assert.assertNull(retrievedQueueStatsAfterDelete);
    }

    @Test
    public void testSaveWithNullQueueName() {
        QueueStats queueStats = new QueueStats();
        queueStats.setTenantId(tenantId);
        queueStats.setQueueName(null);
        queueStats.setServiceId(StringUtils.randomAlphabetic(8));

        Assertions.assertThrows(DataValidationException.class, () -> {
            queueStatsService.save(tenantId, queueStats);
        });
    }

    @Test
    public void testSaveWithNullServiceId() {
        QueueStats queueStats = new QueueStats();
        queueStats.setTenantId(tenantId);
        queueStats.setQueueName(StringUtils.randomAlphabetic(8));
        queueStats.setServiceId(null);

        Assertions.assertThrows(DataValidationException.class, () -> {
            queueStatsService.save(tenantId, queueStats);
        });
    }

    @Test
    public void testFindByTenantIdAndNameAndServiceId() {
        QueueStats queueStats = new QueueStats();
        queueStats.setTenantId(tenantId);
        queueStats.setQueueName(StringUtils.randomAlphabetic(8));
        queueStats.setServiceId(StringUtils.randomAlphabetic(8));
        QueueStats savedQueueStats = queueStatsService.save(tenantId, queueStats);

        QueueStats queueStats2 = new QueueStats();
        queueStats2.setTenantId(tenantId);
        queueStats2.setQueueName(StringUtils.randomAlphabetic(8));
        queueStats2.setServiceId(StringUtils.randomAlphabetic(8));
        queueStatsService.save(tenantId, queueStats2);

        QueueStats retrievedQueueStatsById = queueStatsService.findByTenantIdAndNameAndServiceId(tenantId, queueStats.getQueueName(), queueStats.getServiceId());
        assertThat(retrievedQueueStatsById).isEqualTo(savedQueueStats);
    }

}
