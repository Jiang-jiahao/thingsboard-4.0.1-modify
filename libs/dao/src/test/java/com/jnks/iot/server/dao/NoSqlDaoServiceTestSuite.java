package com.jnks.iot.server.dao;

import org.junit.extensions.cpsuite.ClasspathSuite;
import org.junit.extensions.cpsuite.ClasspathSuite.ClassnameFilters;
import org.junit.runner.RunWith;

@RunWith(ClasspathSuite.class)
@ClassnameFilters({
        "com.jnks.iot.server.dao.service.*.nosql.*ServiceNoSqlTest",
})
public class NoSqlDaoServiceTestSuite extends AbstractNoSqlContainer {

}
