package com.jnks.iot.server.dao;

import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import com.jnks.iot.server.common.stats.StatsFactory;
import com.jnks.iot.server.dao.config.DedicatedEventsJpaDaoConfig;
import com.jnks.iot.server.dao.config.JpaDaoConfig;
import com.jnks.iot.server.dao.config.SqlTsDaoConfig;
import com.jnks.iot.server.dao.config.SqlTsLatestDaoConfig;
import com.jnks.iot.server.dao.service.DaoSqlTest;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {JpaDaoConfig.class, SqlTsDaoConfig.class, SqlTsLatestDaoConfig.class, DedicatedEventsJpaDaoConfig.class})
@DaoSqlTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestExecutionListeners({
        DependencyInjectionTestExecutionListener.class,
        DirtiesContextTestExecutionListener.class})
public abstract class AbstractDaoServiceTest {

    @MockBean(answer = Answers.RETURNS_MOCKS)
    StatsFactory statsFactory;

}
