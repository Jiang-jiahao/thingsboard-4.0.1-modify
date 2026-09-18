package org.thingsboard.server.dao.cassandra;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.thingsboard.server.dao.util.NoSqlAnyDao;

@Component("CassandraCluster")
@NoSqlAnyDao
public class CassandraCluster extends AbstractCassandraCluster {

    @Value("${cassandra.keyspace_name}")
    private String keyspaceName;

    @PostConstruct
    public void init() {
        super.init(keyspaceName);
    }

}
